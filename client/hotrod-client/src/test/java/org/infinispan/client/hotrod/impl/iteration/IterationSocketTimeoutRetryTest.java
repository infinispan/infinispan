package org.infinispan.client.hotrod.impl.iteration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.AdvancedCache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.server.core.iteration.IterableIterationResult;
import org.infinispan.server.core.iteration.IterationInitializationContext;
import org.infinispan.server.core.iteration.IterationManager;
import org.infinispan.server.core.iteration.IterationState;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Verifies that a remote iteration recovers from a transient socket timeout on the iteration start operation, and
 * that a server which never answers eventually produces an error instead of being retried forever.
 *
 * @see <a href="https://github.com/infinispan/infinispan/issues/18483">#18483</a>
 */
@Test(groups = "functional", testName = "client.hotrod.iteration.IterationSocketTimeoutRetryTest")
public class IterationSocketTimeoutRetryTest extends SingleHotRodServerTest {

   private static final int SOCKET_TIMEOUT = 1_000;
   private static final int MAX_RETRIES = 2;
   private static final int NUM_ENTRIES = 50;
   private static final int BATCH_SIZE = 10;

   private DelayingIterationManager delayingIterationManager;
   private IterationManager originalIterationManager;
   // The whole class shares one Hot Rod server, and activeIterations() is server wide, so iterations left behind
   // by an earlier method would otherwise be attributed to this one.
   private int activeIterationsBaseline;

   @BeforeMethod(alwaysRun = true)
   public void recordIterationBaseline() {
      activeIterationsBaseline = hotrodServer.getIterationManager().activeIterations();
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      builder.socketTimeout(SOCKET_TIMEOUT).maxRetries(MAX_RETRIES);
      return new RemoteCacheManager(builder.build());
   }

   @AfterMethod(alwaysRun = true)
   public void releaseDelays() {
      if (delayingIterationManager != null) {
         delayingIterationManager.release();
         delayingIterationManager = null;
      }
      if (originalIterationManager != null) {
         TestingUtil.replaceField(originalIterationManager, "iterationManager", hotrodServer, HotRodServer.class);
         originalIterationManager = null;
      }
   }

   /**
    * Installs a wrapper around the server's iteration manager that blocks the first {@code delayedStarts} calls to
    * {@link IterationManager#start} until the test releases them. The client gives up after {@link #SOCKET_TIMEOUT},
    * which is exactly the transient timeout reported in #18483.
    */
   private void delayIterationStarts(int delayedStarts) {
      install(new DelayingIterationManager(hotrodServer.getIterationManager(), delayedStarts, 0, 0));
   }

   /**
    * Lets {@code servedNexts} calls to {@link IterationManager#next} through, then stalls the following
    * {@code delayedNexts} of them past the client socket timeout. Used to time out in the middle of an iteration,
    * after entries have already been emitted to the subscriber.
    */
   private void delayIterationNexts(int servedNexts, int delayedNexts) {
      install(new DelayingIterationManager(hotrodServer.getIterationManager(), 0, servedNexts, delayedNexts));
   }

   private void install(DelayingIterationManager manager) {
      originalIterationManager = hotrodServer.getIterationManager();
      delayingIterationManager = manager;
      TestingUtil.replaceField(delayingIterationManager, "iterationManager", hotrodServer, HotRodServer.class);
   }

   private Map<String, String> populate(RemoteCache<String, String> cache) {
      Map<String, String> expected = new HashMap<>(NUM_ENTRIES);
      for (int i = 0; i < NUM_ENTRIES; i++) {
         expected.put("key-" + i, "value-" + i);
      }
      cache.putAll(expected);
      return expected;
   }

   public void testIterationRecoversFromTransientStartTimeout() {
      RemoteCache<String, String> cache = remoteCacheManager.getCache();
      Map<String, String> expected = populate(cache);

      // Only the very first iteration start is held back, the retry is served normally.
      delayIterationStarts(1);

      List<Map.Entry<Object, Object>> collected = new ArrayList<>(NUM_ENTRIES);
      try (CloseableIterator<Map.Entry<Object, Object>> iterator = cache.retrieveEntries(null, null, BATCH_SIZE)) {
         iterator.forEachRemaining(collected::add);
      }

      assertEquals(1, delayingIterationManager.delayedStarts(), "The start operation should have been delayed once");
      // Exactly one stalled attempt plus one retry that is served normally.
      assertEquals(2, delayingIterationManager.totalStarts(),
            "Expected the stalled start plus exactly one retry");

      // All entries are returned, with the expected values.
      Map<Object, Object> actual = new HashMap<>();
      List<Object> keys = new ArrayList<>(collected.size());
      for (Map.Entry<Object, Object> entry : collected) {
         actual.put(entry.getKey(), entry.getValue());
         keys.add(entry.getKey());
      }
      assertEquals(expected, actual, "The retried iteration did not return every entry with its expected value");
      // The timed out attempt must not cause any entry to be emitted twice.
      assertEquals(NUM_ENTRIES, keys.size(),
            "Expected exactly " + NUM_ENTRIES + " entries after the retry, duplicates: " + duplicatesOf(keys));
      assertEquals(NUM_ENTRIES, new HashSet<>(keys).size(),
            "Duplicate keys were returned after the retry: " + duplicatesOf(keys));

      // Release the stalled start so its late response reaches the client, which must then close that iteration.
      releaseDelays();
      assertNoLeakedIterations();
   }

   public void testPermanentlyUnavailableServerFailsInsteadOfRetryingForever() {
      RemoteCache<String, String> cache = remoteCacheManager.getCache();
      populate(cache);

      // Every start is held back, so no attempt can ever succeed.
      delayIterationStarts(Integer.MAX_VALUE);

      List<Map.Entry<Object, Object>> collected = new ArrayList<>();
      Throwable thrown = null;
      try (CloseableIterator<Map.Entry<Object, Object>> iterator = cache.retrieveEntries(null, null, BATCH_SIZE)) {
         iterator.forEachRemaining(collected::add);
         fail("Iteration should have failed against a server that never answers, got " + collected.size() + " entries");
      } catch (RuntimeException e) {
         thrown = e;
      }

      assertNotNull(thrown, "Iteration should have failed against a server that never answers the start");
      assertInstanceOf(SocketTimeoutException.class, rootCause(thrown),
            "Expected the socket timeout to be reported, got: " + thrown);

      // The configured retry limit is honoured: one initial attempt plus maxRetries retries, and no more.
      assertEquals(MAX_RETRIES + 1, delayingIterationManager.totalStarts(),
            "Iteration start should have been attempted maxRetries + 1 times");

      releaseDelays();
      assertNoLeakedIterations();
   }

   /**
    * A timeout in the middle of an iteration restarts it from scratch. Entries already emitted must not be
    * emitted a second time.
    */
   public void testTransientNextTimeoutFailsWithoutRestartingOrDuplicating() {
      RemoteCache<String, String> cache = remoteCacheManager.getCache();
      populate(cache);

      // Serve one batch, then stall the following next so the client times out mid-iteration.
      delayIterationNexts(1, 1);

      List<Map.Entry<Object, Object>> collected = new ArrayList<>(NUM_ENTRIES);
      Throwable thrown = null;
      try (CloseableIterator<Map.Entry<Object, Object>> iterator = cache.retrieveEntries(null, null, BATCH_SIZE)) {
         iterator.forEachRemaining(collected::add);
      } catch (RuntimeException e) {
         thrown = e;
      }

      assertEquals(1, delayingIterationManager.delayedNexts(), "The next operation should have been delayed once");
      // An established iteration is never restarted: restarting it would re-read entries already emitted, and
      // nothing deduplicates them when the client has no topology (NoOpSegmentKeyTracker).
      assertNotNull(thrown, "A timeout on an established iteration should be propagated, not retried");
      assertInstanceOf(SocketTimeoutException.class, rootCause(thrown),
            "Expected the socket timeout to be reported, got: " + thrown);
      assertEquals(1, delayingIterationManager.totalStarts(),
            "The iteration must not be restarted after it has already delivered entries");

      // Whatever was handed to the subscriber before the failure must still be free of duplicates.
      List<Object> keys = new ArrayList<>(collected.size());
      for (Map.Entry<Object, Object> entry : collected) {
         keys.add(entry.getKey());
      }
      assertEquals(keys.size(), new HashSet<>(keys).size(),
            "Duplicate entries were emitted before the failure, duplicates: " + duplicatesOf(keys));

      releaseDelays();
      assertNoLeakedIterations();
   }

   /**
    * A server that answers the iteration start but never the subsequent next must not be restarted in a loop.
    * Before the fix this spun until the harness ran out of stalls, because every restart reset the retry budget.
    */
   public void testPermanentNextTimeoutTerminatesInsteadOfRetryingForever() {
      RemoteCache<String, String> cache = remoteCacheManager.getCache();
      populate(cache);

      // Serve one batch, then stall every subsequent next. A restart loop would consume all of these.
      delayIterationNexts(1, 50);

      Throwable thrown = null;
      try (CloseableIterator<Map.Entry<Object, Object>> iterator = cache.retrieveEntries(null, null, BATCH_SIZE)) {
         iterator.forEachRemaining(e -> { });
      } catch (RuntimeException e) {
         thrown = e;
      }

      assertNotNull(thrown, "Iteration should have failed instead of restarting forever");
      assertInstanceOf(SocketTimeoutException.class, rootCause(thrown),
            "Expected the socket timeout to be reported, got: " + thrown);
      // Every restart issues a fresh start, so a restart loop shows up as a growing start count.
      assertEquals(1, delayingIterationManager.totalStarts(),
            "The iteration was restarted after a next timeout, which risks duplicates and an unbounded loop");

      releaseDelays();
      assertNoLeakedIterations();
   }

   private static List<Object> duplicatesOf(List<Object> keys) {
      Set<Object> seen = new HashSet<>();
      List<Object> duplicates = new ArrayList<>();
      for (Object key : keys) {
         if (!seen.add(key)) {
            duplicates.add(key);
         }
      }
      return duplicates;
   }

   /**
    * Asserts this test method left no iteration open on the server, relative to the count recorded before it ran.
    */
   private void assertNoLeakedIterations() {
      eventuallyEquals(activeIterationsBaseline, () -> hotrodServer.getIterationManager().activeIterations());
   }

   private static Throwable rootCause(Throwable t) {
      Throwable cause = t;
      while (cause.getCause() != null && cause.getCause() != cause) {
         cause = cause.getCause();
      }
      return cause;
   }

   /**
    * Delegating {@link IterationManager} that blocks {@link #start} until released, so that the client hits its
    * socket timeout while the server is still working on the request.
    */
   private static class DelayingIterationManager implements IterationManager {
      // Long enough that the client always gives up first, short enough that the server thread is released even if
      // the test never gets to call release().
      private static final long STALL_MILLIS = SOCKET_TIMEOUT * 3L;

      private final IterationManager delegate;
      private final CompletableFuture<Void> gate = new CompletableFuture<>();
      private final AtomicInteger remainingStartDelays;
      private final AtomicInteger remainingServedNexts;
      private final AtomicInteger remainingNextDelays;
      private final AtomicInteger totalStarts = new AtomicInteger();
      private final AtomicInteger delayedStarts = new AtomicInteger();
      private final AtomicInteger delayedNexts = new AtomicInteger();

      DelayingIterationManager(IterationManager delegate, int delayedStarts, int servedNexts, int delayedNexts) {
         this.delegate = delegate;
         this.remainingStartDelays = new AtomicInteger(delayedStarts);
         this.remainingServedNexts = new AtomicInteger(servedNexts);
         this.remainingNextDelays = new AtomicInteger(delayedNexts);
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

      /** Stalls the calling server thread until the test releases the gate, or {@link #STALL_MILLIS} elapses. */
      private void stall() {
         try {
            gate.get(STALL_MILLIS, TimeUnit.MILLISECONDS);
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
         totalStarts.incrementAndGet();
         if (remainingStartDelays.getAndDecrement() > 0) {
            delayedStarts.incrementAndGet();
            stall();
         }
         return delegate.start(cache, segments, filterConverterFactory, filterConverterParams, valueMediaType, batch,
               metadata, guarantee, ctx);
      }

      @Override
      public IterableIterationResult next(String iterationId, int batch) {
         if (remainingServedNexts.getAndDecrement() <= 0 && remainingNextDelays.getAndDecrement() > 0) {
            delayedNexts.incrementAndGet();
            stall();
         }
         return delegate.next(iterationId, batch);
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
}
