package com.carrieropt.routing.controller;

import com.carrieropt.routing.model.RouteRequest;
import com.carrieropt.routing.model.RouteResponse;
import com.carrieropt.routing.service.RoutingService;
import com.google.i18n.phonenumbers.NumberParseException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
public class RouteController {

    private final RoutingService routingService;

    public RouteController(RoutingService routingService) {
        this.routingService = routingService;
    }

    @GetMapping("/health")
    public Mono<String> health() {
        return Mono.just("ok");
    }

    @PostMapping("/route")
    public Mono<ResponseEntity<RouteResponse>> route(@RequestBody RouteRequest request) {
        if (request.dialedNumber() == null || request.dialedNumber().isBlank()) {
            return Mono.just(ResponseEntity.badRequest().build());
        }
        return routingService.route(request.dialedNumber(), request.defaultRegion())
                .map(ResponseEntity::ok)
                .onErrorResume(NumberParseException.class,
                        e -> Mono.just(ResponseEntity.badRequest().build()))
                .onErrorResume(e -> Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build()));
    }
}
