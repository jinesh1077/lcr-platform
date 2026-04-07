package com.carrieropt.routing.model;

public record RouteCandidate(
        String carrierId,
        double costPerMin,
        double effectiveCost,
        double healthPenalty,
        int rank
) {}
