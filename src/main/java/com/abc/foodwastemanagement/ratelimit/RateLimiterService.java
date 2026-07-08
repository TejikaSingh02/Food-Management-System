package com.abc.foodwastemanagement.ratelimit;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

import lombok.RequiredArgsConstructor;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

/**
 * RateLimiterService
 * In-memory Token Bucket implementation using ConcurrentHashMap (no Redis dependency).
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class RateLimiterService {

    private final RateLimitingProperties properties;
    private final ConcurrentHashMap<String, RateLimitBucket> storage = new ConcurrentHashMap<>();

    /**
     * Enforces rate limiting in-memory.
     */
    public void checkRateLimit(String key) {
        try {
            long now = Instant.now().getEpochSecond();
            RateLimitBucket bucket = storage.get(key);

            // Clean up expired buckets (basic TTL logic)
            if (bucket != null) {
                long ttlSeconds = resolveTtlSeconds(key);
                if (now - bucket.getLastRefillTimestamp() > ttlSeconds) {
                    storage.remove(key);
                    bucket = null;
                }
            }

            // FIRST REQUEST
            if (bucket == null) {
                int capacity = resolveCapacity(key);
                RateLimitBucket newBucket = new RateLimitBucket(capacity - 1, now);
                storage.put(key, newBucket);
                return;
            }

            // REFILL
            long elapsedSeconds = now - bucket.getLastRefillTimestamp();
            int refillInterval = resolveRefillIntervalSeconds(key);
            int tokensToAdd = (int) (elapsedSeconds / refillInterval);

            if (tokensToAdd > 0) {
                int capacity = resolveCapacity(key);
                int updatedTokens = Math.min(capacity, bucket.getTokens() + tokensToAdd);
                bucket.setTokens(updatedTokens);
                bucket.setLastRefillTimestamp(
                    bucket.getLastRefillTimestamp() + (long) tokensToAdd * refillInterval);
            }

            // CHECK LIMIT
            if (bucket.getTokens() < 1) {
                throw new AccessDeniedException("RATE_LIMIT_EXCEEDED");
            }

            // CONSUME
            bucket.setTokens(bucket.getTokens() - 1);
            storage.put(key, bucket);

        } catch (AccessDeniedException e) {
            throw e;
        } catch (Exception e) {
            log.warn("Rate limiting skipped due to internal failure. Key={}, Reason={}",
                    key, e.getClass().getSimpleName());
            return;
        }
    }

    /**
     * Calculates Retry-After value (seconds).
     */
    public long getRetryAfterSeconds(String key) {
        RateLimitBucket bucket = storage.get(key);
        if (bucket == null) {
            return 0;
        }

        long now = Instant.now().getEpochSecond();
        int refillInterval = resolveRefillIntervalSeconds(key);
        long nextRefillTime = bucket.getLastRefillTimestamp() + refillInterval;
        return Math.max(nextRefillTime - now, 1);
    }

    /**
     * Provides data for X-RateLimit-* headers.
     */
    public RateLimitInfo getRateLimitInfo(String key) {
        RateLimitBucket bucket = storage.get(key);
        long now = Instant.now().getEpochSecond();
        int capacity = resolveCapacity(key);
        int refillInterval = resolveRefillIntervalSeconds(key);

        if (bucket == null) {
            return new RateLimitInfo(capacity, capacity, now);
        }

        long nextReset = bucket.getLastRefillTimestamp() + refillInterval;
        return new RateLimitInfo(capacity, bucket.getTokens(), nextReset);
    }

    /* =================================================
    RESOLUTION METHODS (CONFIG-DRIVEN)
    ================================================= */
    private String extractProfile(String key) {
        return key.split("-")[1];
    }

    private int resolveCapacity(String key) {
        String profile = extractProfile(key);
        return properties.getProfiles().get(profile).getCapacity();
    }

    private int resolveRefillIntervalSeconds(String key) {
        String profile = extractProfile(key);
        return properties.getProfiles().get(profile).getRefillIntervalSeconds();
    }

    private long resolveTtlSeconds(String key) {
        String profile = extractProfile(key);
        return properties.getProfiles().get(profile).getTtlSeconds();
    }
}
