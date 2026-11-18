package org.infinispan.client.hotrod.impl.iteration;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.server.core.iteration.IterableIterationResult;
import org.infinispan.server.core.iteration.IterationInitializationContext;
import org.infinispan.server.core.iteration.IterationManager;
import org.infinispan.server.core.iteration.IterationState;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;

/**
 * Delegating {@link IterationManager} that blocks {@link #start} or {@link #next} until the test releases it, so
 * that a Hot Rod client reaches its socket timeout while the server is still working on the request. This makes a
 * timeout deterministic, without a sleep or an assumption about how fast the server is.
 *
 * @see IterationSocketTimeoutRetryTest
 * @see SegmentedIterationSocketTimeoutRetryTest
 */
class DelayingIterationManager implements IterationManager {

   private final IterationManager delegate;
   private final CompletableFuture<Void> gate = new CompletableFuture<>();
   // Long enough that the client always gives up first, short enough that the server thread is released even if
   // the test never gets to call release().
   private final long stallMillis;
   private final AtomicInteger remainingStartDelays;
   private final AtomicInteger remainingServedNexts;
   private final AtomicInteger remainingNextDelays;
   private final AtomicInteger totalStarts = new AtomicInteger();
   private final AtomicInteger delayedStarts = new AtomicInteger();
   private final AtomicInteger delayedNexts = new AtomicInteger();
   // Calls currently inside this wrapper, so a test can tell when the released ones have reached the delegate.
   private final AtomicInteger callsInFlight = new AtomicInteger();

   private DelayingIterationManager(IterationManager delegate, long stallMillis, int delayedStarts, int servedNexts,
                                    int delayedNexts) {
      this.delegate = delegate;
      this.stallMillis = stallMillis;
      this.remainingStartDelays = new AtomicInteger(delayedStarts);
      this.remainingServedNexts = new AtomicInteger(servedNexts);
      this.remainingNextDelays = new AtomicInteger(delayedNexts);
   }

   /**
    * Replaces the iteration manager of {@code server} with a wrapper that holds back the first
    * {@code delayedStarts} calls to {@link IterationManager#start}, and that lets {@code servedNexts} calls to
    * {@link IterationManager#next} through before holding back the following {@code delayedNexts} of them.
    */
   static DelayingIterationManager installOn(HotRodServer server, long stallMillis, int delayedStarts,
                                             int servedNexts, int delayedNexts) {
      DelayingIterationManager manager = new DelayingIterationManager(server.getIterationManager(), stallMillis,
            delayedStarts, servedNexts, delayedNexts);
      TestingUtil.replaceField(manager, "iterationManager", server, HotRodServer.class);
      return manager;
   }

   /** Puts the original iteration manager back, so the server is left as it was found. */
   void uninstall(HotRodServer server) {
      TestingUtil.replaceField(delegate, "iterationManager", server, HotRodServer.class);
   }

   void release() {
      gate.complete(null);
   }

   int totalStarts() {
      return totalStarts.get();
   }

   int delayedStarts() {
      return delayedStarts.get();
   }

   int delayedNexts() {
      return delayedNexts.get();
   }

   int callsInFlight() {
      return callsInFlight.get();
   }

   /** Stalls the calling server thread until the test releases the gate, or {@code stallMillis} elapses. */
   private void stall() {
      try {
         gate.get(stallMillis, TimeUnit.MILLISECONDS);
      } catch (TimeoutException ignored) {
         // Deliberate: the client has long since timed out, let the server thread go.
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
         throw new IllegalStateException(e);
      }
   }

   @Override
   public IterationState start(AdvancedCache cache, BitSet segments, String filterConverterFactory,
                               List<byte[]> filterConverterParams, MediaType valueMediaType, int batch,
                               boolean metadata, DeliveryGuarantee guarantee, IterationInitializationContext ctx) {
      callsInFlight.incrementAndGet();
      try {
         totalStarts.incrementAndGet();
         if (remainingStartDelays.getAndDecrement() > 0) {
            delayedStarts.incrementAndGet();
            stall();
         }
         return delegate.start(cache, segments, filterConverterFactory, filterConverterParams, valueMediaType, batch,
               metadata, guarantee, ctx);
      } finally {
         callsInFlight.decrementAndGet();
      }
   }

   @Override
   public IterableIterationResult next(String iterationId, int batch) {
      callsInFlight.incrementAndGet();
      try {
         if (remainingServedNexts.getAndDecrement() <= 0 && remainingNextDelays.getAndDecrement() > 0) {
            delayedNexts.incrementAndGet();
            stall();
         }
         return delegate.next(iterationId, batch);
      } finally {
         callsInFlight.decrementAndGet();
      }
   }

   @Override
   public IterationState close(String iterationId) {
      return delegate.close(iterationId);
   }

   @Override
   public void addKeyValueFilterConverterFactory(String name, KeyValueFilterConverterFactory factory) {
      delegate.addKeyValueFilterConverterFactory(name, factory);
   }

   @Override
   public void removeKeyValueFilterConverterFactory(String name) {
      delegate.removeKeyValueFilterConverterFactory(name);
   }

   @Override
   public int activeIterations() {
      return delegate.activeIterations();
   }
}
