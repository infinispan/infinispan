package org.infinispan.client.rest.impl.jdk.auth;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-request authentication state, created once per request and threaded through the
 * authenticators so that per-exchange state does not leak across concurrent requests.
 *
 * <p>Only per-request state lives here. Genuinely shared, cross-request caches (the digest
 * challenge cache, the localuser session token, the active auto-detect mechanism) stay on the
 * authenticator instances.</p>
 *
 * <p>Fields are package-private so the same-package authenticators access them directly.
 * Counters and collections that are written in an async {@code thenApply} callback and read in
 * the next synchronous {@code authenticate} (different threads) use atomics / concurrent sets for
 * visibility; within a single request the accesses are strictly sequential, so no locking is
 * required.</p>
 */
public final class AuthRequestState {

   // --- Digest: per-challenge nonce bookkeeping ---
   volatile String lastNonce;
   final AtomicLong nonceCount = new AtomicLong();
   volatile String cnonce;

   // Whether the request's previous attempt was a preauthenticated one (digest).
   volatile boolean preauthenticated;

   // --- LocalUser: per-request retry budget for the challenge-response loop ---
   final AtomicInteger retryCount = new AtomicInteger();

   // --- AutoDetect: mechanisms challenged / declined during this request's exchange ---
   final Set<String> challengedMechanisms = ConcurrentHashMap.newKeySet();
   final Set<String> attemptedMechanisms = ConcurrentHashMap.newKeySet();
}
