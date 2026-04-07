package com.carrieropt.routing.model;

import java.util.List;

public record RouteResponse(
        String dialedNumber,
        String matchedPrefix,
        List<RouteCandidate> candidates
) {}
