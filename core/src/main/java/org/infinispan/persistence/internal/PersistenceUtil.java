package org.infinispan.persistence.internal;

import static org.infinispan.util.logging.Log.CONFIG;
import static org.infinispan.util.logging.Log.PERSISTENCE;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import java.util.function.Predicate;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.reactive.RxJavaInterop;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.CustomStoreConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;

/**
 * Persistence Utility that is useful for internal classes. Normally methods that require non public classes, such as
 * PersistenceManager, should go in here.
 * @author William Burns
 * @since 9.4
 */
public class PersistenceUtil {

   private static final Log log = LogFactory.getLog(PersistenceUtil.class);

   private static final int SEGMENT_NOT_PROVIDED = -1;

   public static <K, V> InternalCacheEntry<K,V> loadAndStoreInDataContainer(DataContainer<K, V> dataContainer,
         final PersistenceManager persistenceManager, K key, final InvocationContext ctx, final TimeService timeService,
         final AtomicReference<Boolean> isLoaded) {
      return loadAndStoreInDataContainer(dataContainer, SEGMENT_NOT_PROVIDED, persistenceManager, key, ctx, timeService,
            isLoaded);
   }

   public static <K, V> InternalCacheEntry<K,V> loadAndStoreInDataContainer(DataContainer<K, V> dataContainer, int segment,
         final PersistenceManager persistenceManager, K key, final InvocationContext ctx, final TimeService timeService,
         final AtomicReference<Boolean> isLoaded) {
      return loadAndComputeInDataContainer(dataContainer, segment, persistenceManager, key, ctx, timeService, null, isLoaded);
   }

   public static <K, V> InternalCacheEntry<K,V> loadAndComputeInDataContainer(DataContainer<K, V> dataContainer,
         int segment, final PersistenceManager persistenceManager, K key, final InvocationContext ctx,
         final TimeService timeService, DataContainer.ComputeAction<K, V> action) {
      return loadAndComputeInDataContainer(dataContainer, segment, persistenceManager, key, ctx, timeService, action, null);
   }

   private static <K, V> InternalCacheEntry<K, V> loadAndComputeInDataContainer(DataContainer<K, V> dataContainer,
         int segment, final PersistenceManager persistenceManager, K key, final InvocationContext ctx,
         final TimeService timeService, DataContainer.ComputeAction<K, V> action, final AtomicReference<Boolean> isLoaded) {
      final ByRef<Boolean> expired = new ByRef<>(null);

      DataContainer.ComputeAction<K, V> computeAction = (k, oldEntry, factory) -> {
         InternalCacheEntry<K, V> entryToUse;
         //under the lock, check if the entry exists in the DataContainer
         if (oldEntry != null) {
            if (oldEntry.canExpire() && oldEntry.isExpired(timeService.wallClockTime())) {
               // If it was expired we can check CacheLoaders - since they can have different
               // metadata than a store
               MarshallableEntry<K, V> loaded = loadAndCheckExpiration(persistenceManager, key, segment, ctx, false);
               if (loaded != null) {
                  if (isLoaded != null) {
                     isLoaded.set(Boolean.TRUE); //loaded!
                  }
                  entryToUse = convert(loaded, factory);

               } else {
                  if (isLoaded != null) {
                     isLoaded.set(Boolean.FALSE); //not loaded
                  }
                  expired.set(Boolean.TRUE);
                  // Return the original entry - so it doesn't remove expired entry early
                  return oldEntry;
               }
            } else {
               if (isLoaded != null) {
                  isLoaded.set(null); //no attempt to load
               }
               entryToUse = oldEntry;
            }
         } else {
            // There was no entry in memory so check all the stores to see if it is there
            MarshallableEntry<K, V> loaded = loadAndCheckExpiration(persistenceManager, key, segment, ctx, true);
            if (loaded != null) {
               if (isLoaded != null) {
                  isLoaded.set(Boolean.TRUE); //loaded!
               }
               entryToUse = convert(loaded, factory);
            } else {if (isLoaded != null) {
               isLoaded.set(Boolean.FALSE); //not loaded
            }

               entryToUse = null;
            }
         }

         if (action != null) {
            return action.compute(k, entryToUse, factory);
         } else {
            return entryToUse;
         }
      };
      InternalCacheEntry<K,V> entry;
      if (segment != SEGMENT_NOT_PROVIDED && dataContainer instanceof InternalDataContainer) {
         entry = ((InternalDataContainer<K, V>) dataContainer).compute(segment, key, computeAction);
      } else {
         entry = dataContainer.compute(key, computeAction);
      }
      if (expired.get() == Boolean.TRUE) {
         return null;
      } else {
         return entry;
      }
   }

   public static <K, V> MarshallableEntry<K, V> loadAndCheckExpiration(PersistenceManager persistenceManager, Object key,
                                                                       int segment, InvocationContext context) {
      return loadAndCheckExpiration(persistenceManager, key, segment, context, true);
   }

   private static <K, V> MarshallableEntry<K, V> loadAndCheckExpiration(PersistenceManager persistenceManager, Object key,
                                                                        int segment, InvocationContext context, boolean includeStores) {
      final MarshallableEntry<K, V> loaded;
      if (segment != SEGMENT_NOT_PROVIDED) {
         loaded = CompletionStages.join(persistenceManager.loadFromAllStores(key, segment, context.isOriginLocal(), includeStores));
      } else {
         loaded = CompletionStages.join(persistenceManager.loadFromAllStores(key, context.isOriginLocal(), includeStores));
      }
      if (log.isTraceEnabled()) {
         log.tracef("Loaded %s for key %s from persistence.", loaded, key);
      }
      return loaded;
   }

   public static <K, V> InternalCacheEntry<K, V> convert(MarshallableEntry<K, V> loaded, InternalEntryFactory factory) {
      Metadata metadata = loaded.getMetadata();
      InternalCacheEntry<K, V> ice;
      if (metadata != null) {
         ice = factory.create(loaded.getKey(), loaded.getValue(), metadata, loaded.created(), metadata.lifespan(),
               loaded.lastUsed(), metadata.maxIdle());
      } else {
         ice = factory.create(loaded.getKey(), loaded.getValue(), (Metadata) null, loaded.created(), -1, loaded.lastUsed(), -1);
      }
      ice.setInternalMetadata(loaded.getInternalMetadata());
      return ice;
   }

   public static <K> Predicate<? super K> combinePredicate(IntSet segments, KeyPartitioner keyPartitioner, Predicate<? super K> filter) {
      if (segments != null) {
         Predicate<Object> segmentFilter = k -> segments.contains(keyPartitioner.getSegment(k));
         return filter == null ? segmentFilter : filter.and(segmentFilter);
      }
      return filter;
   }

   public static <R> Flowable<R> parallelizePublisher(IntSet segments, Scheduler scheduler,
         IntFunction<Publisher<R>> publisherFunction) {
      Flowable<Publisher<R>> flowable = Flowable.fromStream(segments.intStream().mapToObj(publisherFunction));
      // We internally support removing rxjava empty flowables - don't waste thread on them
      flowable = flowable.filter(f -> f != Flowable.empty());
      return flowable.parallel()
            .runOn(scheduler)
            .flatMap(RxJavaInterop.identityFunction())
            .sequential();
   }

   /**
    * @deprecated This method is only public for use with prior Store classes, use
    * {@link #storeFromConfiguration(StoreConfiguration)} when dealing with {@link NonBlockingStore} instances
    */
   @SuppressWarnings("unchecked")
   public static <T> T createStoreInstance(StoreConfiguration config) {
      Class<?> classBasedOnConfigurationAnnotation = getClassBasedOnConfigurationAnnotation(config);
      try {
         Object instance = Util.getInstance(classBasedOnConfigurationAnnotation);
         return (T) instance;
      } catch (CacheConfigurationException unableToInstantiate) {
         throw PERSISTENCE.unableToInstantiateClass(config.getClass());
      }
   }

   public static <K, V> NonBlockingStore<K, V> storeFromConfiguration(StoreConfiguration cfg) {
      final Object bareInstance = PersistenceUtil.createStoreInstance(cfg);
      return (NonBlockingStore<K, V>) bareInstance;
   }

   private static Class<?> getClassBasedOnConfigurationAnnotation(StoreConfiguration cfg) {
      ConfigurationFor annotation = cfg.getClass().getAnnotation(ConfigurationFor.class);
      Class<?> classAnnotation = null;
      if (annotation == null) {
         if (cfg instanceof CustomStoreConfiguration) {
            classAnnotation = ((CustomStoreConfiguration)cfg).customStoreClass();
         }
      } else {
         classAnnotation = annotation.value();
      }
      if (classAnnotation == null) {
         throw CONFIG.loaderConfigurationDoesNotSpecifyLoaderClass(cfg.getClass().getName());
      }
      return classAnnotation;
   }
}
