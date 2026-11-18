package org.infinispan.client.hotrod.impl.iteration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.server.core.iteration.IterationManager;
import org.testng.annotations.AfterMethod;
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

   // Long enough that the client always gives up first, short enough that the server thread is released even if
   // the test never gets to release it.
   private static final long STALL_MILLIS = SOCKET_TIMEOUT * 3L;

   private DelayingIterationManager delayingIterationManager;

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
         delayingIterationManager.uninstall(hotrodServer);
         delayingIterationManager = null;
      }
   }

   /**
    * Installs a wrapper around the server's iteration manager that blocks the first {@code delayedStarts} calls to
    * {@link IterationManager#start} until the test releases them. The client gives up after {@link #SOCKET_TIMEOUT},
    * which is exactly the transient timeout reported in #18483.
    */
   private void delayIterationStarts(int delayedStarts) {
      delayingIterationManager = DelayingIterationManager.installOn(hotrodServer, STALL_MILLIS, delayedStarts, 0, 0);
   }

   /**
    * Lets {@code servedNexts} calls to {@link IterationManager#next} through, then stalls the following
    * {@code delayedNexts} of them past the client socket timeout. Used to time out in the middle of an iteration,
    * after entries have already been emitted to the subscriber.
    */
   private void delayIterationNexts(int servedNexts, int delayedNexts) {
      delayingIterationManager = DelayingIterationManager.installOn(hotrodServer, STALL_MILLIS, 0, servedNexts,
            delayedNexts);
   }

   private Map<String, String> populate(RemoteCache<String, String> cache) {
      Map<String, String> expected = new HashMap<>(NUM_ENTRIES);
      for (int i = 0; i < NUM_ENTRIES; i++) {
         expected.put("key-" + i, "value-" + i);
      }
      cache.putAll(expected);
      return expected;
   }

   /**
    * An explicit interval over the first half of the segment space, which is what the segment aware tests request.
    * <p>
    * Note the interval does not narrow the result here: this cache is local and not transactional and has no
    * segmented store, so {@code Configurations.needSegments} is false and every key maps to segment 0. That is
    * deliberate, since what an explicit interval changes for this issue is the client side path, see
    * {@link #testIterationWithExplicitSegmentsRecoversFromTransientStartTimeout}.
    */
   private Set<Integer> explicitSegmentInterval() {
      int numSegments = cacheManager.getCache().getCacheConfiguration().clustering().hash().numSegments();
      Set<Integer> segments = new HashSet<>(numSegments / 2);
      for (int i = 0; i < numSegments / 2; i++) {
         segments.add(i);
      }
      return segments;
   }

   private List<Map.Entry<Object, Object>> collect(RemoteCache<String, String> cache, Set<Integer> segments) {
      List<Map.Entry<Object, Object>> collected = new ArrayList<>(NUM_ENTRIES);
      try (CloseableIterator<Map.Entry<Object, Object>> iterator = cache.retrieveEntries(null, segments, BATCH_SIZE)) {
         iterator.forEachRemaining(collected::add);
      }
      return collected;
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
      releaseStalledCalls();
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

      releaseStalledCalls();
      assertNoLeakedIterations();
   }

   /**
    * The same transient start timeout as {@link #testIterationRecoversFromTransientStartTimeout}, but with an
    * explicit segment interval, which takes the other branch of {@link RemotePublisher#subscribe}.
    * <p>
    * With no segments the publisher drives one handler whose supplier hands out one further target after a failure,
    * so the retry is a second target on the same handler. With segments it builds a fresh handler per
    * {@code repeatUntil} cycle and loops until every requested segment has been completed, so the retry is a whole
    * new cycle with a new handler. That is why the retry budget has to live on the publisher: a counter held by the
    * handler would be thrown away with it on every cycle and would bound nothing.
    */
   public void testIterationWithExplicitSegmentsRecoversFromTransientStartTimeout() {
      RemoteCache<String, String> cache = remoteCacheManager.getCache();
      Map<String, String> expected = populate(cache);
      Set<Integer> requestedSegments = explicitSegmentInterval();

      // Only the very first iteration start is held back, the retry is served normally.
      delayIterationStarts(1);

      List<Map.Entry<Object, Object>> collected = collect(cache, requestedSegments);

      assertEquals(1, delayingIterationManager.delayedStarts(), "The start operation should have been delayed once");
      // Exactly one stalled attempt plus one repeatUntil cycle that is served normally.
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
      // A repeated cycle that did not complete its segments would iterate them again, so this is also what
      // catches the retry re-reading what the abandoned attempt would have read.
      assertEquals(NUM_ENTRIES, keys.size(),
            "Expected exactly " + NUM_ENTRIES + " entries after the retry, duplicates: " + duplicatesOf(keys));
      assertEquals(NUM_ENTRIES, new HashSet<>(keys).size(),
            "Duplicate keys were returned after the retry: " + duplicatesOf(keys));

      // Release the stalled start so its late response reaches the client, which must then close that iteration.
      releaseStalledCalls();
      assertNoLeakedIterations();
   }

   /**
    * The retry budget must also bound the {@code repeatUntil} loop of the explicit segment path. Nothing in that
    * loop terminates by itself: every cycle that fails leaves the requested segments outstanding, so the loop
    * repeats, and only the publisher wide counter stops it.
    */
   public void testExplicitSegmentsPermanentlyUnavailableServerFailsInsteadOfRetryingForever() {
      RemoteCache<String, String> cache = remoteCacheManager.getCache();
      populate(cache);
      Set<Integer> requestedSegments = explicitSegmentInterval();

      // Every start is held back, so no attempt can ever succeed.
      delayIterationStarts(Integer.MAX_VALUE);

      List<Map.Entry<Object, Object>> collected = new ArrayList<>();
      Throwable thrown = null;
      try (CloseableIterator<Map.Entry<Object, Object>> iterator =
                 cache.retrieveEntries(null, requestedSegments, BATCH_SIZE)) {
         iterator.forEachRemaining(collected::add);
         fail("Iteration should have failed against a server that never answers, got " + collected.size() + " entries");
      } catch (RuntimeException e) {
         thrown = e;
      }

      assertNotNull(thrown, "Iteration should have failed against a server that never answers the start");
      assertInstanceOf(SocketTimeoutException.class, rootCause(thrown),
            "Expected the socket timeout to be reported, got: " + thrown);

      // The configured retry limit is honoured across the repeatUntil cycles: one initial attempt plus maxRetries.
      assertEquals(MAX_RETRIES + 1, delayingIterationManager.totalStarts(),
            "Iteration start should have been attempted maxRetries + 1 times");

      releaseStalledCalls();
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

      releaseStalledCalls();
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

      releaseStalledCalls();
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
    * Releases the stalled calls and waits until they have returned from the delegate. A stalled {@code start} only
    * creates its server side iteration once it is released, so returning before that would let
    * {@link #assertNoLeakedIterations()} read {@code activeIterations()} before the iteration we want it to account
    * for even exists.
    */
   private void releaseStalledCalls() {
      DelayingIterationManager manager = delayingIterationManager;
      if (manager != null) {
         manager.release();
         eventuallyEquals(0, manager::callsInFlight);
      }
   }

   /**
    * Asserts no iteration is left open on the server. This test class owns the Hot Rod server exclusively and no
    * other client connects to it, so every iteration it ever sees is one of ours and the count has to settle back
    * at zero, both for the iteration the test drove and for the abandoned ones its timeouts left behind.
    */
   private void assertNoLeakedIterations() {
      eventuallyEquals(0, () -> hotrodServer.getIterationManager().activeIterations());
   }

   private static Throwable rootCause(Throwable t) {
      Throwable cause = t;
      while (cause.getCause() != null && cause.getCause() != cause) {
         cause = cause.getCause();
      }
      return cause;
   }
}
