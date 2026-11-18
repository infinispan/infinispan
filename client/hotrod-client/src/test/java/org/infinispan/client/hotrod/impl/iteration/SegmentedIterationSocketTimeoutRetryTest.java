package org.infinispan.client.hotrod.impl.iteration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * The segment aware half of {@link IterationSocketTimeoutRetryTest}. An iteration that requests an explicit segment
 * interval takes the other branch of {@link RemotePublisher#subscribe}: rather than one handler whose supplier
 * hands out a further target after a failure, the publisher builds a handler per target and repeats the whole
 * cycle until every requested segment has been completed. The retry budget therefore has to be held by the
 * publisher, since a counter held by a handler would be discarded with it on every cycle.
 * <p>
 * This runs on a distributed cluster rather than on the local cache of {@link IterationSocketTimeoutRetryTest}: a
 * cache that does not need segments maps every key to segment 0, which would leave the requested interval covering
 * the whole cache and the iteration single target. Here the interval selects a strict subset and the publisher
 * really does have one target per server.
 *
 * @see <a href="https://github.com/infinispan/infinispan/issues/18483">#18483</a>
 */
@Test(groups = "functional", testName = "client.hotrod.iteration.SegmentedIterationSocketTimeoutRetryTest")
public class SegmentedIterationSocketTimeoutRetryTest extends MultiHotRodServersTest {

   private static final int SOCKET_TIMEOUT = 1_000;
   private static final int MAX_RETRIES = 3;
   private static final int SERVERS = 2;
   private static final int NUM_ENTRIES = 200;
   private static final int BATCH_SIZE = 10;
   // Long enough that the client always gives up first, short enough that the server thread is released even if
   // the test never gets to release it.
   private static final long STALL_MILLIS = SOCKET_TIMEOUT * 3L;

   private final Map<HotRodServer, DelayingIterationManager> delayed = new HashMap<>();

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(SERVERS, getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
   }

   @Override
   protected int maxRetries() {
      return MAX_RETRIES;
   }

   @Override
   protected ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int serverPort) {
      return super.createHotRodClientConfigurationBuilder(host, serverPort).socketTimeout(SOCKET_TIMEOUT);
   }

   @AfterMethod(alwaysRun = true)
   public void releaseDelays() {
      delayed.forEach((server, manager) -> {
         manager.release();
         manager.uninstall(server);
      });
      delayed.clear();
   }

   /**
    * Holds back the first {@code delayedStarts} iteration starts of every server, so that whichever target the
    * publisher sends to is stalled past the client socket timeout.
    */
   private void delayIterationStartsOnEveryServer(int delayedStarts) {
      for (HotRodServer server : servers) {
         delayed.put(server, DelayingIterationManager.installOn(server, STALL_MILLIS, delayedStarts, 0, 0));
      }
   }

   private int totalStarts() {
      return delayed.values().stream().mapToInt(DelayingIterationManager::totalStarts).sum();
   }

   private int delayedStarts() {
      return delayed.values().stream().mapToInt(DelayingIterationManager::delayedStarts).sum();
   }

   /**
    * An explicit interval over the first half of the segment space. The cluster is distributed, so this selects a
    * strict subset of the entries and its segments are spread over both servers.
    */
   private Set<Integer> explicitSegmentInterval() {
      int numSegments = cache(0).getCacheConfiguration().clustering().hash().numSegments();
      return IntStream.range(0, numSegments / 2).boxed().collect(Collectors.toSet());
   }

   private Map<String, String> populate(RemoteCache<String, String> cache) {
      Map<String, String> entries = new HashMap<>(NUM_ENTRIES);
      for (int i = 0; i < NUM_ENTRIES; i++) {
         entries.put("key-" + i, "value-" + i);
      }
      cache.putAll(entries);
      return entries;
   }

   private List<Map.Entry<Object, Object>> collect(RemoteCache<String, String> cache, Set<Integer> segments) {
      List<Map.Entry<Object, Object>> collected = new ArrayList<>();
      try (CloseableIterator<Map.Entry<Object, Object>> iterator =
                 cache.retrieveEntries(null, segments, BATCH_SIZE)) {
         iterator.forEachRemaining(collected::add);
      }
      return collected;
   }

   private static Map<Object, Object> asMap(List<Map.Entry<Object, Object>> entries) {
      Map<Object, Object> map = new HashMap<>(entries.size());
      for (Map.Entry<Object, Object> entry : entries) {
         map.put(entry.getKey(), entry.getValue());
      }
      return map;
   }

   private static List<Object> keysOf(List<Map.Entry<Object, Object>> entries) {
      List<Object> keys = new ArrayList<>(entries.size());
      for (Map.Entry<Object, Object> entry : entries) {
         keys.add(entry.getKey());
      }
      return keys;
   }

   /**
    * Releases the stalled calls and waits until they have returned from the delegate. A stalled {@code start} only
    * creates its server side iteration once it is released, so returning before that would let
    * {@link #assertNoLeakedIterations()} read the counts before the iterations it has to account for even exist.
    */
   private void releaseStalledCalls() {
      delayed.values().forEach(DelayingIterationManager::release);
      eventuallyEquals(0, () -> delayed.values().stream().mapToInt(DelayingIterationManager::callsInFlight).sum());
   }

   /**
    * Asserts no iteration is left open on any server. This test class owns the cluster exclusively and no other
    * client connects to it, so every iteration it ever sees is one of ours.
    */
   private void assertNoLeakedIterations() {
      eventuallyEquals(0, () -> servers.stream().mapToInt(s -> s.getIterationManager().activeIterations()).sum());
   }

   public void testSegmentedIterationRecoversFromTransientStartTimeout() {
      RemoteCache<String, String> cache = clients.get(0).getCache();
      populate(cache);
      Set<Integer> requestedSegments = explicitSegmentInterval();

      // Establish what the interval holds with nothing stalled. Taking it from the server rather than recomputing
      // the key to segment mapping in the test keeps the later comparison exact, and it also makes sure the client
      // knows the topology before the interesting run, so the publisher really does build one target per server.
      List<Map.Entry<Object, Object>> undisturbed = collect(cache, requestedSegments);
      Map<Object, Object> expected = asMap(undisturbed);
      assertEquals(undisturbed.size(), expected.size(), "The undisturbed iteration already returned duplicates");
      assertTrue(expected.size() > 0 && expected.size() < NUM_ENTRIES,
            "The requested segment interval should select a strict subset, selected " + expected.size() + " of "
                  + NUM_ENTRIES);
      assertNoLeakedIterations();

      // Only the first iteration start of each server is held back, every later one is served normally.
      delayIterationStartsOnEveryServer(1);

      List<Map.Entry<Object, Object>> collected = collect(cache, requestedSegments);

      assertEquals(SERVERS, delayedStarts(), "Every server should have had its first start delayed");
      // The stalled starts, plus the repeatUntil cycles that complete the outstanding segments. The retried
      // segments may be served by either server, so only the lower bound is fixed.
      assertTrue(totalStarts() > SERVERS, "The stalled starts should have been retried, starts: " + totalStarts());

      List<Object> keys = keysOf(collected);
      assertEquals(expected, asMap(collected),
            "The retried iteration did not return every entry of the requested segments with its expected value");
      // A retried cycle that re-read segments an earlier one had already completed would show up here.
      assertEquals(expected.size(), keys.size(),
            "Expected exactly " + expected.size() + " entries after the retry, duplicates: " + duplicatesOf(keys));
      assertEquals(expected.size(), new HashSet<>(keys).size(),
            "Duplicate keys were returned after the retry: " + duplicatesOf(keys));

      releaseStalledCalls();
      assertNoLeakedIterations();
   }

   /**
    * The retry budget must bound the {@code repeatUntil} loop too. Nothing in that loop terminates by itself: a
    * cycle that fails leaves the requested segments outstanding, so the loop repeats, and only the publisher wide
    * counter stops it.
    */
   public void testSegmentedIterationStopsRetryingUnavailableServers() {
      RemoteCache<String, String> cache = clients.get(0).getCache();
      populate(cache);
      Set<Integer> requestedSegments = explicitSegmentInterval();

      // No start can ever succeed, on either server.
      delayIterationStartsOnEveryServer(Integer.MAX_VALUE);

      List<Map.Entry<Object, Object>> collected = new ArrayList<>();
      Throwable thrown = null;
      try (CloseableIterator<Map.Entry<Object, Object>> iterator =
                 cache.retrieveEntries(null, requestedSegments, BATCH_SIZE)) {
         iterator.forEachRemaining(collected::add);
         fail("Iteration should have failed against servers that never answer, got " + collected.size() + " entries");
      } catch (RuntimeException e) {
         thrown = e;
      }

      assertNotNull(thrown, "Iteration should have failed against servers that never answer the start");
      assertInstanceOf(SocketTimeoutException.class, rootCause(thrown),
            "Expected the socket timeout to be reported, got: " + thrown);
      // The failure is retried, and then stops. Every target draws on the single budget the publisher holds, so
      // the first cycle alone consumes one per target and the exact total depends on how the outstanding segments
      // are grouped on the following cycles. Without the fix the first timeout is propagated, one start in total.
      assertTrue(totalStarts() > SERVERS && totalStarts() <= SERVERS + MAX_RETRIES,
            "Iteration start should have been retried and then bounded by maxRetries, starts: " + totalStarts());

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

   private static Throwable rootCause(Throwable t) {
      Throwable cause = t;
      while (cause.getCause() != null && cause.getCause() != cause) {
         cause = cause.getCause();
      }
      return cause;
   }
}
