package org.infinispan.reactive.publisher.impl;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.reactive.publisher.impl.SegmentAwarePublisherSupplier.NotificationWithLost;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestException;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.mockito.stubbing.Answer;
import org.reactivestreams.Publisher;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Verifies how {@link PublisherHandler} closes the publishers it serves for other nodes when the cache stops while
 * a publisher is still being registered.
 * <p>
 * The stop closes a publisher only after its subscription completes. Cancelling it while the register thread is still
 * subscribing makes RxJava report any later upstream error as undeliverable instead of passing it to the handler.
 * The register thread closes such a publisher itself once the subscription returns.
 *
 * @since 16.3
 */
@CleanupAfterMethod
@Test(groups = "functional", testName = "reactive.publisher.impl.PublisherHandlerStopTest")
public class PublisherHandlerStopTest extends MultipleCacheManagersTest {

   private static final int NUM_KEYS = 100;
   private static final int TIMEOUT_SECONDS = 10;

   @Override
   protected void createCacheManagers() {
      // Two owners, so every segment the server serves has another owner to read from.
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(2);
      createClusteredCaches(3, builder);
   }

   public void testStopWhileSubscribingDeliversUpstreamError() throws Exception {
      Cache<String, String> requester = cache(0);
      Cache<String, String> server = cache(1);
      insertKeys(requester);

      CheckPoint checkPoint = new CheckPoint();
      TestException failure = new TestException("Upstream failed after the cache stopped");
      AtomicReference<Throwable> delivered = new AtomicReference<>();
      // The register thread blocks after onSubscribe, still inside subscribe, and fails once released.
      // Not Flowable.just: concatMap over a scalar source calls the mapper before onSubscribe.
      Flowable<NotificationWithLost<Object>> publisher = Flowable.fromIterable(List.of(1))
            .<NotificationWithLost<Object>>concatMap(ignore -> {
               checkPoint.trigger("subscribing");
               checkPoint.awaitStrict("release", TIMEOUT_SECONDS, SECONDS);
               throw failure;
            })
            .doOnError(delivered::set);
      replaceFirstEntryPublisher(server, ignore -> supplierOf(publisher));

      PublisherHandler serverHandler = extractComponent(server, PublisherHandler.class);
      Future<Set<String>> iteration = fork(() -> iterateKeys(requester));
      checkPoint.awaitStrict("subscribing", TIMEOUT_SECONDS, SECONDS);

      fork(() -> server.stop()).get(TIMEOUT_SECONDS, SECONDS);
      checkPoint.triggerForever("release");

      // The error reaches the handler's subscriber instead of being reported as undeliverable.
      eventually(() -> delivered.get() == failure);
      eventually(() -> serverHandler.openPublishers() == 0);
      // The requester reads the server's segments from the other owners.
      assertThat(iteration.get(TIMEOUT_SECONDS, SECONDS)).hasSize(NUM_KEYS);
   }

   public void testStopBeforeSubscribeClosesPublisher() throws Exception {
      Cache<String, String> requester = cache(0);
      Cache<String, String> server = cache(1);
      insertKeys(requester);

      CheckPoint checkPoint = new CheckPoint();
      CountDownLatch cancelled = new CountDownLatch(1);
      // Never completes, so only a cancel closes it.
      Flowable<NotificationWithLost<Object>> publisher = Flowable.<NotificationWithLost<Object>>never()
            .doOnCancel(cancelled::countDown);
      // The register thread blocks after registering the publisher, before subscribing to it.
      replaceFirstEntryPublisher(server, ignore -> {
         checkPoint.trigger("registered");
         checkPoint.awaitStrict("release", TIMEOUT_SECONDS, SECONDS);
         return supplierOf(publisher);
      });

      PublisherHandler serverHandler = extractComponent(server, PublisherHandler.class);
      Future<Set<String>> iteration = fork(() -> iterateKeys(requester));
      checkPoint.awaitStrict("registered", TIMEOUT_SECONDS, SECONDS);

      fork(() -> server.stop()).get(TIMEOUT_SECONDS, SECONDS);
      checkPoint.triggerForever("release");

      // The register thread closes the publisher it subscribed after the stop.
      assertThat(cancelled.await(TIMEOUT_SECONDS, SECONDS)).isTrue();
      eventually(() -> serverHandler.openPublishers() == 0);
      assertThat(iteration.get(TIMEOUT_SECONDS, SECONDS)).hasSize(NUM_KEYS);
   }

   /**
    * Replaces the first entry publisher the cache creates with the given answer. Later publishers are the real ones.
    */
   private static void replaceFirstEntryPublisher(Cache<?, ?> cache, Answer<SegmentAwarePublisherSupplier<Object>> answer) {
      LocalPublisherManager<?, ?> manager = Mocks.replaceComponentWithSpy(cache, LocalPublisherManager.class);
      AtomicBoolean first = new AtomicBoolean(true);
      doAnswer(invocation -> first.getAndSet(false) ? answer.answer(invocation) : invocation.callRealMethod())
            .when(manager).entryPublisher(any(), any(), any(), anyLong(), any(), any());
   }

   private static SegmentAwarePublisherSupplier<Object> supplierOf(Flowable<NotificationWithLost<Object>> publisher) {
      return new SegmentAwarePublisherSupplier<>() {
         @Override
         public Publisher<NotificationWithLost<Object>> publisherWithLostSegments(boolean reuseNotifications) {
            return publisher;
         }

         @Override
         public Publisher<Object> publisherWithoutSegments() {
            throw new UnsupportedOperationException();
         }

         @Override
         public Publisher<Notification<Object>> publisherWithSegments() {
            throw new UnsupportedOperationException();
         }
      };
   }

   private static void insertKeys(Cache<String, String> cache) {
      for (int i = 0; i < NUM_KEYS; i++) {
         cache.put("key-" + i, "value-" + i);
      }
   }

   private static Set<String> iterateKeys(Cache<String, String> cache) {
      Set<String> keys = new HashSet<>();
      try (CacheStream<Map.Entry<String, String>> stream = cache.entrySet().stream()) {
         stream.iterator().forEachRemaining(e -> keys.add(e.getKey()));
      }
      return keys;
   }
}
