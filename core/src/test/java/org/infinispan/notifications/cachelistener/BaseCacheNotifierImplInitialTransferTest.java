package org.infinispan.notifications.cachelistener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.cache.impl.EncoderCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.impl.InternalEntryFactoryImpl;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.MockBasicComponentRegistry;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventManager;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManager;
import org.infinispan.reactive.publisher.impl.Notifications;
import org.infinispan.reactive.publisher.impl.SegmentPublisherSupplier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;


@Test(groups = "unit", testName = "notifications.cachelistener.BaseCacheNotifierImplInitialTransferTest")
public abstract class BaseCacheNotifierImplInitialTransferTest extends AbstractInfinispanTest {
   CacheNotifierImpl n;
   EncoderCache mockCache;
   InvocationContext ctx;
   ClusterPublisherManager mockPublisherManager;

   protected CacheMode cacheMode;

   protected BaseCacheNotifierImplInitialTransferTest(CacheMode mode) {
      if (mode.isDistributed()) {
         throw new IllegalArgumentException("This test only works with non distributed cache modes");
      }
      this.cacheMode = mode;
   }

   private enum Operation {
      PUT(Event.Type.CACHE_ENTRY_MODIFIED) {
         @Override
         public void raiseEvent(CacheNotifier notifier, Object key, Object prevValue, Object newValue,
                                InvocationContext ctx) {
            notifier.notifyCacheEntryModified(key, newValue, null, prevValue, null, true, ctx, null);
            notifier.notifyCacheEntryModified(key, newValue, null, prevValue, null, false, ctx, null);
         }
      }, REMOVE(Event.Type.CACHE_ENTRY_REMOVED) {
         @Override
         public void raiseEvent(CacheNotifier notifier, Object key, Object prevValue, Object newValue,
                                InvocationContext ctx) {
            notifier.notifyCacheEntryRemoved(key, prevValue, null, true, ctx, null);
            notifier.notifyCacheEntryRemoved(key, prevValue, null, false, ctx, null);
         }
      }, CREATE(Event.Type.CACHE_ENTRY_CREATED) {
         @Override
         public void raiseEvent(CacheNotifier notifier, Object key, Object prevValue, Object newValue,
                                InvocationContext ctx) {
            notifier.notifyCacheEntryCreated(key, newValue, null, true, ctx, null);
            notifier.notifyCacheEntryCreated(key, newValue, null, false, ctx, null);
         }
      };

      private final Event.Type type;

      Operation(Event.Type type) {
         this.type = type;
      }

      public Event.Type getType() {
         return type;
      }

      public abstract void raiseEvent(CacheNotifier notifier, Object key, Object prevValue, Object newValue,
                                      InvocationContext ctx);
   }

   @BeforeMethod
   public void setUp() {
      n = new CacheNotifierImpl();
      mockCache = mock(EncoderCache.class);
      EmbeddedCacheManager cacheManager = mock(EmbeddedCacheManager.class);
      when(mockCache.getCacheManager()).thenReturn(cacheManager);
      when(mockCache.getAdvancedCache()).thenReturn(mockCache);
      when(mockCache.getKeyDataConversion()).thenReturn(DataConversion.IDENTITY_KEY);
      when(mockCache.getValueDataConversion()).thenReturn(DataConversion.IDENTITY_VALUE);
      Configuration config = new ConfigurationBuilder().clustering().cacheMode(cacheMode).build();
      GlobalConfiguration globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder().build();
      when(mockCache.getStatus()).thenReturn(ComponentStatus.INITIALIZING);

      mockPublisherManager = mock(ClusterPublisherManager.class);

      ComponentRegistry componentRegistry = mock(ComponentRegistry.class);
      when(mockCache.getComponentRegistry()).thenReturn(componentRegistry);
      MockBasicComponentRegistry mockRegistry = new MockBasicComponentRegistry();
      when(componentRegistry.getComponent(BasicComponentRegistry.class)).thenReturn(mockRegistry);
      mockRegistry.registerMocks(RpcManager.class, CommandsFactory.class);
      mockRegistry.registerMock(KnownComponentNames.INTERNAL_MARSHALLER, Marshaller.class);
      ClusteringDependentLogic.LocalLogic cdl = new ClusteringDependentLogic.LocalLogic();
      cdl.init(null, config, mock(KeyPartitioner.class));
      ClusterEventManager cem = mock(ClusterEventManager.class);
      when(cem.sendEvents(any())).thenReturn(CompletableFutures.completedNull());
      BlockingManager handler = mock(BlockingManager.class);
      when(handler.continueOnNonBlockingThread(any(), any())).thenReturn(CompletableFutures.completedNull());
      TestingUtil.inject(n, mockCache, cdl, config, globalConfig, mockRegistry, mockPublisherManager,
                         new InternalEntryFactoryImpl(), cem, mock(KeyPartitioner.class), handler,
                         TestingUtil.named(KnownComponentNames.ASYNC_NOTIFICATION_EXECUTOR, new WithinThreadExecutor()));
      n.start();
      ctx = new NonTxInvocationContext(null);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testSimpleCacheStartingNonClusterListener() {
//      testSimpleCacheStarting(new StateListenerNotClustered());
//   }

   public void testSimpleCacheStartingClusterListener() {
      testSimpleCacheStarting(new StateListenerClustered());
   }

   private void testSimpleCacheStarting(final StateListener<String, String> listener) {
      final List<CacheEntry<String, String>> initialValues = new ArrayList<>(10);
      for (int i = 0; i < 10; i++) {
         String key = "key-" + i;
         String value = "value-" + i;
         initialValues.add(new ImmortalCacheEntry(key, value));
      }

      when(mockPublisherManager.entryPublisher(any(), any(), any(), anyLong(), any(), anyInt(), any()))
            .thenReturn(wrapCompletionPublisher(Flowable.fromIterable(initialValues)));

      n.addListener(listener);
      verifyEvents(isClustered(listener), listener, initialValues);
   }

   public void testFilterConverterUnusedDuringIteration() {
      testFilterConverterUnusedDuringIteration(new StateListenerClustered());
   }

   private void testFilterConverterUnusedDuringIteration(final StateListener<String, String> listener) {
      final List<CacheEntry<String, String>> initialValues = new ArrayList<CacheEntry<String, String>>(10);
      for (int i = 0; i < 10; i++) {
         String key = "key-" + i;
         String value = "value-" + i;
         initialValues.add(new ImmortalCacheEntry(key, value));
      }

      // Note we don't actually use the filter/converter to retrieve values since it is being mocked, thus we can assert
      // the filter/converter are not used by us

      when(mockPublisherManager.entryPublisher(any(), any(), any(), anyLong(), any(), anyInt(), any()))
            .thenReturn(wrapCompletionPublisher(Flowable.fromIterable(initialValues)));

      CacheEventFilter filter = mock(CacheEventFilter.class, withSettings().serializable());
      CacheEventConverter converter = mock(CacheEventConverter.class, withSettings().serializable());
      n.addListener(listener, filter, converter);
      verifyEvents(isClustered(listener), listener, initialValues);

      verify(filter, never()).accept(any(), any(), any(Metadata.class), any(), any(Metadata.class),
            any(EventType.class));
      verify(converter, never()).convert(any(), any(), any(Metadata.class), any(), any(Metadata.class),
            any(EventType.class));
   }

   public void testMetadataAvailable() {
      final List<CacheEntry<String, String>> initialValues = new ArrayList<>(10);
      for (int i = 0; i < 10; i++) {
         String key = "key-" + i;
         String value = "value-" + i;
         initialValues.add(new TransientMortalCacheEntry(key, value, i, -1, System.currentTimeMillis()));
      }

      // Note we don't actually use the filter/converter to retrieve values since it is being mocked, thus we can assert
      // the filter/converter are not used by us
      when(mockPublisherManager.entryPublisher(any(), any(), any(), anyLong(), any(), anyInt(), any()))
            .thenReturn(wrapCompletionPublisher(Flowable.fromIterable(initialValues)));

      CacheEventFilter filter = mock(CacheEventFilter.class, withSettings().serializable());
      CacheEventConverter converter = mock(CacheEventConverter.class, withSettings().serializable());
      StateListener<String, String> listener = new StateListenerClustered();
      n.addListener(listener, filter, converter);
      verifyEvents(isClustered(listener), listener, initialValues);

      for (CacheEntryEvent<String, String> event : listener.events) {
         String key = event.getKey();
         Metadata metadata = event.getMetadata();
         assertNotNull(metadata);
         assertEquals(metadata.lifespan(), -1);
         assertEquals(metadata.maxIdle(), Long.parseLong(key.substring(4)));
      }
   }

   private void verifyEvents(boolean isClustered, StateListener<String, String> listener,
                             List<CacheEntry<String, String>> expected) {
      assertEquals(listener.events.size(), isClustered ? expected.size() : expected.size() * 2);
      int eventPosition = 0;
      for (CacheEntryEvent<String, String> event : listener.events) {
         // Even checks means it will be post and have a value - note we force every check to be
         // even for clustered since those should always be post
         int position;
         boolean isPost;
         if (isClustered) {
            isPost = true;
            position = eventPosition;
         } else {
            isPost = (eventPosition & 1) == 1;
            position = eventPosition / 2;
         }

         assertEquals(event.getType(), Event.Type.CACHE_ENTRY_CREATED);
         assertEquals(event.getKey(), expected.get(position).getKey());
         assertEquals(event.isPre(), !isPost);
         if (isPost) {
            assertEquals(event.getValue(), expected.get(position).getValue());
         } else {
            assertNull(event.getValue());
         }
         eventPosition++;
      }
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testCreateAfterIterationBeganButNotIteratedValueYetNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
//      testModificationAfterIterationBeganButNotIteratedValueYet(new StateListenerNotClustered(), Operation.CREATE);
//   }

   public void testCreateAfterIterationBeganButNotIteratedValueYetClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
      testModificationAfterIterationBeganButNotIteratedValueYet(new StateListenerClustered(), Operation.CREATE);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testRemoveAfterIterationBeganButNotIteratedValueYetNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
//      testModificationAfterIterationBeganButNotIteratedValueYet(new StateListenerNotClustered(), Operation.REMOVE);
//   }

   public void testRemoveAfterIterationBeganButNotIteratedValueYetClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
      testModificationAfterIterationBeganButNotIteratedValueYet(new StateListenerClustered(), Operation.REMOVE);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testModificationAfterIterationBeganButNotIteratedValueYetNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
//      testModificationAfterIterationBeganButNotIteratedValueYet(new StateListenerNotClustered(), Operation.PUT);
//   }

   public void testModificationAfterIterationBeganButNotIteratedValueYetClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
      testModificationAfterIterationBeganButNotIteratedValueYet(new StateListenerClustered(), Operation.PUT);
   }

   /**
    * This test is to verify that the modification event replaces the current value for the key
    */
   private void testModificationAfterIterationBeganButNotIteratedValueYet(final StateListener<String, String> listener,
                                                                          Operation operation) throws InterruptedException,
         TimeoutException, BrokenBarrierException, ExecutionException {
      final List<CacheEntry<String, String>> initialValues = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
         String key = "key-" + i;
         String value = "value-" + i;
         initialValues.add(new ImmortalCacheEntry(key, value));
      }

      final CyclicBarrier barrier = new CyclicBarrier(2);

      when(mockPublisherManager.entryPublisher(any(), any(), any(), anyLong(), any(), anyInt(), any()))
            .thenReturn(wrapCompletionPublisher(Flowable.defer(() -> {
               barrier.await(10, TimeUnit.SECONDS);
               barrier.await(10, TimeUnit.SECONDS);
               return Flowable.fromIterable(initialValues);
            })));
      Future<Void> future = fork(() -> {
         n.addListener(listener);
         return null;
      });

      barrier.await(10, TimeUnit.SECONDS);
      log.debugf("Synced with publisher, performing operation");

      String prevValue = initialValues.get(3).getValue();
      switch (operation) {
         case REMOVE:
            String key = "key-3";

            n.notifyCacheEntryRemoved(key, prevValue, null, true, ctx, null);
            n.notifyCacheEntryRemoved(key, prevValue, null, false, ctx, null);

            // We shouldn't see the event at all now!
            initialValues.remove(3);
            break;
         case CREATE:
            key = "new-key";
            String value = "new-value";
            n.notifyCacheEntryCreated(key, value, null, true, ctx, null);
            n.notifyCacheEntryCreated(key, value, null, false, ctx, null);

            // Need to add a new value to the end
            initialValues.add(new ImmortalCacheEntry(key, value));
            break;
         case PUT:
            key = "key-3";
            value = "value-3-changed";

            n.notifyCacheEntryModified(key, value, null, prevValue, null, true, ctx, null);
            n.notifyCacheEntryModified(key, value, null, prevValue, null, false, ctx, null);

            // Now remove the old value and put in the new one
            initialValues.remove(3);
            initialValues.add(3, new ImmortalCacheEntry(key, value));
            break;
         default:
            throw new IllegalArgumentException("Unsupported Operation provided " + operation);
      }

      log.debugf("Operation done, let the iteration complete");
      barrier.await(10, TimeUnit.SECONDS);

      future.get(10, TimeUnit.MINUTES);

      verifyEvents(isClustered(listener), listener, initialValues);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testCreateAfterIterationBeganAndIteratedValueNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
//      testModificationAfterIterationBeganAndIteratedValue(new StateListenerNotClustered(), Operation.CREATE);
//   }

   public void testCreateAfterIterationBeganAndIteratedValueClustered() throws Exception {
      testModificationAfterIterationBeganAndIteratedValue(new StateListenerClustered(), Operation.CREATE);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testRemoveAfterIterationBeganAndIteratedValueNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
//      testModificationAfterIterationBeganAndIteratedValue(new StateListenerNotClustered(), Operation.REMOVE);
//   }

   public void testRemoveAfterIterationBeganAndIteratedValueClustered() throws Exception {
      testModificationAfterIterationBeganAndIteratedValue(new StateListenerClustered(), Operation.REMOVE);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testModificationAfterIterationBeganAndIteratedValueNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
//      testModificationAfterIterationBeganAndIteratedValue(new StateListenerNotClustered(), Operation.PUT);
//   }

   public void testModificationAfterIterationBeganAndIteratedValueClustered() throws Exception {
      testModificationAfterIterationBeganAndIteratedValue(new StateListenerClustered(), Operation.PUT);
   }

   /**
    * This test is to verify that the modification event is sent after the creation event is done
    */
   private void testModificationAfterIterationBeganAndIteratedValue(final StateListener<String, String> listener,
                                                                    Operation operation) throws Exception {
      final List<CacheEntry<String, String>> initialValues = new ArrayList<CacheEntry<String, String>>();
      for (int i = 0; i < 10; i++) {
         String key = "key-" + i;
         String value = "value-" + i;
         initialValues.add(new ImmortalCacheEntry(key, value));
      }

      final CyclicBarrier barrier = new CyclicBarrier(2);

      when(mockPublisherManager.entryPublisher(any(), any(), any(), anyLong(), any(), anyInt(), any()))
            .thenReturn(wrapCompletionPublisher(Flowable.fromIterable(initialValues)
                  .doOnComplete(() -> {
                     barrier.await(10, TimeUnit.SECONDS);
                     barrier.await(10, TimeUnit.SECONDS);
                  })));

      Future<Void> future = fork(() -> {
         n.addListener(listener);
         return null;
      });

      barrier.await(10, TimeUnit.SECONDS);

      String key;
      String prevValue;
      String value;

      switch (operation) {
         case REMOVE:
            key = "key-3";
            value = null;
            prevValue = initialValues.get(3).getValue();
            break;
         case CREATE:
            key = "new-key";
            value = "new-value";
            prevValue = null;
            break;
         case PUT:
            key = "key-3";
            value = "key-3-new";
            prevValue = initialValues.get(3).getValue();
            break;
         default:
            throw new IllegalArgumentException("Unsupported Operation provided " + operation);
      }

      operation.raiseEvent(n, key, prevValue, value, ctx);

      // Now let the iteration complete
      barrier.await(10, TimeUnit.SECONDS);

      future.get(10, TimeUnit.MINUTES);

      boolean isClustered = isClustered(listener);

      // We should have 1 or 2 (local) events due to the modification coming after we iterated on it.  Note the value
      // isn't brought up until the iteration is done
      assertEquals(listener.events.size(), isClustered ? initialValues.size() + 1 : (initialValues.size() + 1) * 2);
      int position = 0;
      for (CacheEntry<String, String> expected : initialValues) {
         if (isClustered) {
            CacheEntryEvent<String, String> event = listener.events.get(position);
            assertEquals(event.getType(), Event.Type.CACHE_ENTRY_CREATED);
            assertEquals(event.isPre(), false);
            assertEquals(event.getKey(), expected.getKey());
            assertEquals(event.getValue(), expected.getValue());
         } else {
            CacheEntryEvent<String, String> event = listener.events.get(position * 2);
            assertEquals(event.getType(), Event.Type.CACHE_ENTRY_CREATED);
            assertEquals(event.isPre(), true);
            assertEquals(event.getKey(), expected.getKey());
            assertNull(event.getValue());

            event = listener.events.get((position * 2) + 1);
            assertEquals(event.getType(), Event.Type.CACHE_ENTRY_CREATED);
            assertEquals(event.isPre(), false);
            assertEquals(event.getKey(), expected.getKey());
            assertEquals(event.getValue(), expected.getValue());
         }
         position++;
      }

      // We should have 2 extra events at the end which are our modifications
      if (isClustered) {
         CacheEntryEvent<String, String> event = listener.events.get(position);
         assertEquals(event.getType(), operation.getType());
         assertEquals(event.isPre(), false);
         assertEquals(event.getKey(), key);
         assertEquals(event.getValue(), value);
      } else {
         CacheEntryEvent<String, String> event = listener.events.get(position * 2);
         assertEquals(event.getType(), operation.getType());
         assertEquals(event.isPre(), true);
         assertEquals(event.getKey(), key);
         assertEquals(event.getValue(), prevValue);

         event = listener.events.get((position * 2) + 1);
         assertEquals(event.getType(), operation.getType());
         assertEquals(event.isPre(), false);
         assertEquals(event.getKey(), key);
         assertEquals(event.getValue(), value);
      }
   }

   private boolean isClustered(StateListener listener) {
      return listener.getClass().getAnnotation(Listener.class).clustered();
   }

   protected static abstract class StateListener<K, V> {
      final List<CacheEntryEvent<K, V>> events = new ArrayList<>();
      private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

      @CacheEntryCreated
      @CacheEntryModified
      @CacheEntryRemoved
      public synchronized void onCacheNotification(CacheEntryEvent<K, V> event) {
         log.tracef("Received event: %s", event);
         events.add(event);
      }
   }

   @Listener(includeCurrentState = true, clustered = false)
   private static class StateListenerNotClustered extends StateListener {

   }

   @Listener(includeCurrentState = true, clustered = true)
   private static class StateListenerClustered extends StateListener {

   }

   private SegmentPublisherSupplier<CacheEntry<String, String>> wrapCompletionPublisher(
         Flowable<CacheEntry<String, String>> flowable) {
      return new SegmentPublisherSupplier<CacheEntry<String, String>>() {
         @Override
         public Publisher<CacheEntry<String, String>> publisherWithoutSegments() {
            return flowable;
         }

         @Override
         public Publisher<Notification<CacheEntry<String, String>>> publisherWithSegments() {
            // This may need to be changed if a test requires the actual segment
            return flowable.map(entry -> (Notification<CacheEntry<String, String>>) Notifications.value(entry, 0))
                  .concatWith(Single.just(Notifications.segmentComplete(0)));
         }
      };
   }
}
