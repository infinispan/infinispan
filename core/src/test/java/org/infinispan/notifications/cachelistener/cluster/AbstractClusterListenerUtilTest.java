package org.infinispan.notifications.cachelistener.cluster;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.concurrent.IsolationLevel;
import org.mockito.AdditionalAnswers;
import org.mockito.stubbing.Answer;

/**
 * Base class to be used for cluster listener tests for both tx and nontx caches
 *
 * @author wburns
 * @since 7.0
 */
public abstract class AbstractClusterListenerUtilTest extends MultipleCacheManagersTest {
   protected final static String CACHE_NAME = "cluster-listener";
   protected final static String FIRST_VALUE = "first-value";
   protected final static String SECOND_VALUE = "second-value";

   protected ConfigurationBuilder builderUsed;
   protected SerializationContextInitializer sci;
   protected final boolean tx;
   protected final CacheMode cacheMode;

   protected ControlledTimeService ts0;
   protected ControlledTimeService ts1;
   protected ControlledTimeService ts2;

   protected AbstractClusterListenerUtilTest(boolean tx, CacheMode cacheMode) {
      // Have to have cleanup after each method since listeners need to be cleaned up
      cleanup = CleanupPhase.AFTER_METHOD;
      this.tx = tx;
      this.cacheMode = cacheMode;
      this.sci = new ListenerSerializationContextImpl();
   }

   protected void addClusteredCacheManager() {
      // First we add the new node, but block the dist exec execution
      log.info("Adding a new node ..");
      EmbeddedCacheManager manager = addClusterEnabledCacheManager(sci);
      manager.defineConfiguration(CACHE_NAME, builderUsed.build());
      log.info("Added a new node");
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(cacheMode);
      if (tx) {
         builderUsed.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
         builderUsed.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
      }
      builderUsed.expiration().disableReaper();
      createClusteredCaches(3, CACHE_NAME, sci, builderUsed);
      injectTimeServices();
   }

   protected void injectTimeServices() {
      ts0 = new ControlledTimeService();
      TestingUtil.replaceComponent(manager(0), TimeService.class, ts0, true);
      ts1 = new ControlledTimeService();
      TestingUtil.replaceComponent(manager(1), TimeService.class, ts1, true);
      ts2 = new ControlledTimeService();
      TestingUtil.replaceComponent(manager(2), TimeService.class, ts2, true);
   }

   void advanceTimeServices(long time, TimeUnit unit) {
      long millis = unit.toMillis(time);

      ts0.advance(millis);
      ts1.advance(millis);
      ts2.advance(millis);
   }

   @Listener(clustered = true)
   protected class ClusterListener {
      List<CacheEntryEvent> events = Collections.synchronizedList(new ArrayList<>());

      @CacheEntryCreated
      public void onCreatedEvent(CacheEntryCreatedEvent event) {
         onCacheEvent(event);
      }

      @CacheEntryModified
      public void onModifiedEvent(CacheEntryModifiedEvent event) {
         onCacheEvent(event);
      }

      @CacheEntryRemoved
      public void onRemoveEvent(CacheEntryRemovedEvent event) {
         onCacheEvent(event);
      }

      @CacheEntryExpired
      public void onExpireEvent(CacheEntryExpiredEvent event) {
         onCacheEvent(event);
      }

      void onCacheEvent(CacheEntryEvent event) {
         log.debugf("Adding new cluster event %s", event);
         events.add(event);
      }

      protected boolean hasIncludeState() {
         return false;
      }
   }

   @Listener(clustered = true, includeCurrentState = true)
   protected class ClusterListenerWithIncludeCurrentState extends ClusterListener {
      protected boolean hasIncludeState() {
         return true;
      }
   }

   public static class LifespanFilter<K, V> implements KeyValueFilter<K, V> {

      @ProtoFactory
      LifespanFilter(long lifespan) {
         this.lifespan = lifespan;
      }

      @ProtoField(number = 1, defaultValue = "-1")
      final long lifespan;

      @Override
      public boolean accept(K key, V value, Metadata metadata) {
         if (metadata == null) {
            return false;
         }
         // Only accept entities with a lifespan longer than ours
         return metadata.lifespan() > lifespan;
      }
   }

   @ProtoName("NewLifespanLargerFilter")
   protected static class NewLifespanLargerFilter<K, V> implements CacheEventFilter<K, V> {
      @Override
      public boolean accept(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
         // If neither metadata is provided dont' raise the event (this will preclude all creations and removals)
         if (oldMetadata == null || newMetadata == null) {
            return true;
         }
         // Only accept entities with a lifespan that is now longer
         return newMetadata.lifespan() > oldMetadata.lifespan();
      }
   }

   public static class LifespanConverter implements CacheEventConverter<Object, String, Object> {

      @ProtoField(number = 1, defaultValue = "false")
      final boolean returnOriginalValueOrNull;

      @ProtoField(number = 2, defaultValue = "-1")
      final long lifespanThreshold;

      @ProtoFactory
      LifespanConverter(boolean returnOriginalValueOrNull, long lifespanThreshold) {
         this.returnOriginalValueOrNull = returnOriginalValueOrNull;
         this.lifespanThreshold = lifespanThreshold;
      }

      @Override
      public Object convert(Object key, String oldValue, Metadata oldMetadata, String newValue, Metadata newMetadata, EventType eventType) {
         if (newMetadata != null) {
            long metaLifespan = newMetadata.lifespan();
            if (metaLifespan > lifespanThreshold) {
               return metaLifespan;
            }
         }
         if (returnOriginalValueOrNull) {
            return newValue;
         }
         return null;
      }
   }

   public static class StringTruncator implements CacheEventConverter<Object, String, String> {

      @ProtoField(number = 1, defaultValue = "0")
      int beginning;

      @ProtoField(number = 2, defaultValue = "0")
      int length;

      @ProtoFactory
      StringTruncator(int beginning, int length) {
         this.beginning = beginning;
         this.length = length;
      }

      @Override
      public String convert(Object key, String oldValue, Metadata oldMetadata, String newValue, Metadata newMetadata, EventType eventType) {
         if (newValue != null && newValue.length() > beginning + length) {
            return newValue.substring(beginning, beginning + length);
         } else {
            return newValue;
         }
      }
   }

   @ProtoName("StringAppender")
   public static class StringAppender implements CacheEventConverter<Object, String, String> {
      @Override
      public String convert(Object key, String oldValue, Metadata oldMetadata, String newValue, Metadata newMetadata, EventType eventType) {
         return oldValue + (oldMetadata != null ? oldMetadata.lifespan() : "null") + newValue + (newMetadata != null ? newMetadata.lifespan() : "null");
      }
   }

   public static class FilterConverter implements CacheEventFilterConverter<Object, Object, Object> {

      @ProtoField(number = 1, defaultValue = "false")
      final boolean throwExceptionOnNonFilterAndConverterMethods;

      @ProtoField(number = 2)
      final String convertedValue;

      @ProtoFactory
      FilterConverter(boolean throwExceptionOnNonFilterAndConverterMethods, String convertedValue) {
         this.throwExceptionOnNonFilterAndConverterMethods = throwExceptionOnNonFilterAndConverterMethods;
         this.convertedValue = convertedValue;
      }

      @Override
      public Object filterAndConvert(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
         return convertedValue;
      }

      @Override
      public Object convert(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
         if (throwExceptionOnNonFilterAndConverterMethods) {
            throw new AssertionError("Method should not have been invoked!");
         }

         return filterAndConvert(key, oldValue, oldMetadata, oldValue, oldMetadata, eventType);
      }

      @Override
      public boolean accept(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
         if (throwExceptionOnNonFilterAndConverterMethods) {
            throw new AssertionError("Method should not have been invoked!");
         }
         return filterAndConvert(key, oldValue, oldMetadata, oldValue, oldMetadata, eventType) != null;
      }
   }

   protected void verifySimpleInsertion(Cache<Object, String> cache, Object key, String value, Long lifespan,
                                      ClusterListener listener, Object expectedValue) {
      if (lifespan != null) {
         cache.put(key, value, lifespan, TimeUnit.MILLISECONDS);
      } else {
         cache.put(key, value);
      }
      verifySimpleInsertionEvents(listener, key, expectedValue);
   }

   protected void verifySimpleModification(Cache<Object, String> cache, Object key, String value, Long lifespan,
                                        ClusterListener listener, Object expectedValue) {
      if (lifespan != null) {
         cache.put(key, value, lifespan, TimeUnit.MILLISECONDS);
      } else {
         cache.put(key, value);
      }
      verifySimpleModificationEvents(listener, key, expectedValue);
   }

   protected void verifySimpleInsertionEvents(ClusterListener listener, Object key, Object expectedValue) {
      assertEquals(1, listener.events.size());
      CacheEntryEvent event = listener.events.get(0);

      assertEquals(Event.Type.CACHE_ENTRY_CREATED, event.getType());
      assertEquals(key, event.getKey());
      assertEquals(expectedValue, event.getValue());
   }

   protected void verifySimpleModificationEvents(ClusterListener listener, Object key, Object expectedValue) {
      assertEquals(listener.hasIncludeState() ? 2 : 1, listener.events.size());
      CacheEntryEvent event = listener.events.get(listener.hasIncludeState() ? 1 :0);

      assertEquals(Event.Type.CACHE_ENTRY_MODIFIED, event.getType());
      assertEquals(key, event.getKey());
      assertEquals(expectedValue, event.getValue());
   }

   protected void verifySimpleExpirationEvents(ClusterListener listener, int expectedNumEvents, Object key, Object expectedValue) {
      eventually(() -> listener.events.size() >= expectedNumEvents);

      CacheEntryEvent event = listener.events.get(expectedNumEvents - 1); //the index starts from 0

      assertEquals(Event.Type.CACHE_ENTRY_EXPIRED, event.getType());
      assertEquals(key, event.getKey());
      assertEquals(expectedValue, event.getValue());
   }

   protected void waitUntilListenerInstalled(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      CacheNotifier cn = TestingUtil.extractComponent(cache, CacheNotifier.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(cn);
      ClusterCacheNotifier mockNotifier = mock(ClusterCacheNotifier.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(invocation -> {
         // Wait for main thread to sync up
         checkPoint.trigger("pre_add_listener_invoked_" + cache);
         // Now wait until main thread lets us through
         checkPoint.awaitStrict("pre_add_listener_release_" + cache, 10, TimeUnit.SECONDS);

         try {
            return forwardedAnswer.answer(invocation);
         } finally {
            // Wait for main thread to sync up
            checkPoint.trigger("post_add_listener_invoked_" + cache);
            // Now wait until main thread lets us through
            checkPoint.awaitStrict("post_add_listener_release_" + cache, 10, TimeUnit.SECONDS);
         }
      }).when(mockNotifier).addFilteredListener(notNull(), nullable(CacheEventFilter.class), nullable(CacheEventConverter.class), any(Set.class));
      TestingUtil.replaceComponent(cache, CacheNotifier.class, mockNotifier, true);
   }

   protected void waitUntilNotificationRaised(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      CacheNotifier cn = TestingUtil.extractComponent(cache, CacheNotifier.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(cn);
      CacheNotifier mockNotifier = mock(CacheNotifier.class,
                                        withSettings().extraInterfaces(ClusterCacheNotifier.class)
                                                      .defaultAnswer(forwardedAnswer));
      Answer answer = invocation -> {
         // Wait for main thread to sync up
         checkPoint.trigger("pre_raise_notification_invoked");
         // Now wait until main thread lets us through
         checkPoint.awaitStrict("pre_raise_notification_release", 10, TimeUnit.SECONDS);

         try {
            return forwardedAnswer.answer(invocation);
         } finally {
            // Wait for main thread to sync up
            checkPoint.trigger("post_raise_notification_invoked");
            // Now wait until main thread lets us through
            checkPoint.awaitStrict("post_raise_notification_release", 10, TimeUnit.SECONDS);
         }
      };
      doAnswer(answer).when(mockNotifier).notifyCacheEntryCreated(any(), any(), any(Metadata.class), eq(false),
            any(InvocationContext.class), any(FlagAffectedCommand.class));
      doAnswer(answer).when(mockNotifier).notifyCacheEntryModified(any(), any(), any(Metadata.class), any(),
            any(Metadata.class), anyBoolean(),
            any(InvocationContext.class), any(FlagAffectedCommand.class));
      doAnswer(answer).when(mockNotifier).notifyCacheEntryRemoved(any(), any(), any(Metadata.class), eq(false),
                                                                  any(InvocationContext.class),
                                                                  any(FlagAffectedCommand.class));
      TestingUtil.replaceComponent(cache, CacheNotifier.class, mockNotifier, true);
   }

   protected void waitUntilViewChangeOccurs(final CacheContainer cacheContainer, final String uniqueId, final CheckPoint checkPoint) {
      CacheManagerNotifier cmn = TestingUtil.extractGlobalComponent(cacheContainer, CacheManagerNotifier.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(cmn);
      CacheManagerNotifier mockNotifier = mock(CacheManagerNotifier.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(invocation -> {
         // Wait for main thread to sync up
         checkPoint.trigger("pre_view_listener_invoked_" + uniqueId);
         // Now wait until main thread lets us through
         checkPoint.awaitStrict("pre_view_listener_release_" + uniqueId, 10, TimeUnit.SECONDS);

         try {
            return forwardedAnswer.answer(invocation);
         } finally {
            checkPoint.trigger("post_view_listener_invoked_" + uniqueId);
         }
      }).when(mockNotifier).notifyViewChange(anyList(), anyList(), any(Address.class), anyInt());
      TestingUtil.replaceComponent(cacheContainer, CacheManagerNotifier.class, mockNotifier, true);
   }

   @AutoProtoSchemaBuilder(
         dependsOn = org.infinispan.test.TestDataSCI.class,
         includeClasses = {
               AbstractClusterListenerUtilTest.FilterConverter.class,
               AbstractClusterListenerUtilTest.LifespanConverter.class,
               AbstractClusterListenerUtilTest.LifespanFilter.class,
               AbstractClusterListenerUtilTest.NewLifespanLargerFilter.class,
               AbstractClusterListenerUtilTest.StringAppender.class,
               AbstractClusterListenerUtilTest.StringTruncator.class,
         },
         schemaFileName = "core.listeners.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.core.notifications")
   interface ListenerSerializationContext extends SerializationContextInitializer {
   }
}
