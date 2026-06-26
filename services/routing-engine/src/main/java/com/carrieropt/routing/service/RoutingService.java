package com.carrieropt.routing.service;

import com.carrieropt.routing.model.RouteCandidate;
import com.carrieropt.routing.model.RouteResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;

@Service
public class RoutingService {

    private static final String ACTIVE_BUFFER_KEY = "trie:active";
    private static final int TOP_N = 3;

    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final Timer routeTimer;

    public RoutingService(ReactiveStringRedisTemplate redis, ObjectMapper mapper, MeterRegistry registry) {
        this.redis = redis;
        this.mapper = mapper;
        this.routeTimer = Timer.builder("route_latency").register(registry);
    }

    public Mono<RouteResponse> route(String rawNumber, String defaultRegion) {
        return Mono.fromCallable(() -> normalize(rawNumber, defaultRegion))
                .flatMap(this::lookup)
                .transformDeferred(m -> routeTimer.record(() -> m));
    }

    private String normalize(String raw, String region) throws NumberParseException {
        PhoneNumberUtil util = PhoneNumberUtil.getInstance();
        String defaultRegion = region != null && !region.isBlank() ? region : "US";
        String trimmed = raw.trim();
        Phonenumber.PhoneNumber num;
        if (trimmed.startsWith("+")) {
            num = util.parse(trimmed, defaultRegion);
        } else if (trimmed.startsWith("0")) {
            num = util.parse(trimmed, defaultRegion);
        } else {
            try {
                num = util.parse("+" + trimmed, defaultRegion);
            } catch (NumberParseException e) {
                num = util.parse(trimmed, defaultRegion);
            }
        }
        return util.format(num, PhoneNumberUtil.PhoneNumberFormat.E164).substring(1);
    }

    private Mono<RouteResponse> lookup(String e164) {
        return redis.opsForValue().get(ACTIVE_BUFFER_KEY)
                .defaultIfEmpty("A")
                .flatMap(buffer -> findLongestPrefix(buffer, e164)
                        .flatMap(match -> buildCandidates(buffer, e164, match)));
    }

    private Mono<String> findLongestPrefix(String buffer, String number) {
        List<String> keys = new ArrayList<>();
        for (int i = 1; i <= number.length(); i++) {
            keys.add("trie:" + buffer + ":" + number.substring(0, i));
        }
        Collections.reverse(keys);

        return Flux.fromIterable(keys)
                .flatMap(key -> redis.opsForValue().get(key)
                        .filter(v -> v != null && !v.isBlank() && hasCarriers(v))
                        .map(v -> key.substring(key.lastIndexOf(':') + 1)))
                .next()
                .defaultIfEmpty(number.length() >= 3 ? number.substring(0, 3) : number);
    }

    private boolean hasCarriers(String json) {
        try {
            JsonNode carriers = mapper.readTree(json).path("carriers");
            return carriers.isObject() && carriers.fieldNames().hasNext();
        } catch (Exception e) {
            return false;
        }
    }

    private Mono<RouteResponse> buildCandidates(String buffer, String e164, String prefix) {
        String key = "trie:" + buffer + ":" + prefix;
        return redis.opsForValue().get(key)
                .defaultIfEmpty("{}")
                .flatMapMany(json -> parseCarriers(json))
                .flatMap(entry -> isBlocked(entry.carrierId()).flatMap(blocked -> {
                    if (blocked) return Mono.empty();
                    return getHealthPenalty(entry.carrierId())
                            .map(penalty -> new Scored(entry.carrierId(), entry.cost(), entry.cost() * (1 + penalty), penalty));
                }))
                .collectList()
                .map(scored -> {
                    scored.sort(Comparator.comparingDouble(Scored::effectiveCost));
                    List<RouteCandidate> candidates = new ArrayList<>();
                    for (int i = 0; i < Math.min(TOP_N, scored.size()); i++) {
                        Scored s = scored.get(i);
                        candidates.add(new RouteCandidate(s.carrierId(), s.cost(), s.effectiveCost(), s.penalty(), i + 1));
                    }
                    return new RouteResponse(e164, prefix, candidates);
                });
    }

    private Flux<CarrierEntry> parseCarriers(String json) {
        try {
            JsonNode root = mapper.readTree(json);
            JsonNode carriers = root.path("carriers");
            List<CarrierEntry> entries = new ArrayList<>();
            carriers.fields().forEachRemaining(f ->
                    entries.add(new CarrierEntry(f.getKey(), f.getValue().asDouble())));
            return Flux.fromIterable(entries);
        } catch (Exception e) {
            return Flux.empty();
        }
    }

    private Mono<Boolean> isBlocked(String carrierId) {
        return redis.hasKey("blocklist:" + carrierId);
    }

    private Mono<Double> getHealthPenalty(String carrierId) {
        return redis.opsForValue().get("health:" + carrierId)
                .map(v -> {
                    try {
                        return Double.parseDouble(v);
                    } catch (NumberFormatException e) {
                        return 0.0;
                    }
                })
                .defaultIfEmpty(0.0);
    }

    private record CarrierEntry(String carrierId, double cost) {}
    private record Scored(String carrierId, double cost, double effectiveCost, double penalty) {}
}
