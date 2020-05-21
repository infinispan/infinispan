package org.infinispan.persistence;

import static org.infinispan.util.logging.Log.PERSISTENCE;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalStateConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.SegmentedAdvancedLoadWriteStore;
import org.infinispan.util.logging.Log;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class PersistenceUtil {

   private static final int SEGMENT_NOT_PROVIDED = -1;

   public static <K, V> int count(AdvancedCacheLoader<K, V> acl, Predicate<? super K> filter) {

      // This can't be null
      Long result = singleToValue(Flowable.fromPublisher(acl.publishKeys(filter)).count());
      if (result > Integer.MAX_VALUE) {
         return Integer.MAX_VALUE;
      }
      return result.intValue();
   }

   /**
    * Counts how many entries are present in the segmented store. Only the segments provided will have entries counted.
    * @param salws segmented store containing entries
    * @param segments segments to count entries from
    * @return count of entries that are in the provided segments
    */
   public static int count(SegmentedAdvancedLoadWriteStore<?, ?> salws, IntSet segments) {
      Long result = singleToValue(Flowable.fromPublisher(salws.publishKeys(segments, null)).count());
      if (result > Integer.MAX_VALUE) {
         return Integer.MAX_VALUE;
      }
      return result.intValue();
   }

   // This method is blocking - but only invoked by tests or user code
   @SuppressWarnings("checkstyle:ForbiddenMethod")
   private static <E> E singleToValue(Single<E> single) {
      return single.blockingGet();
   }

   // This method is blocking - but only invoked by tests or user code
   @SuppressWarnings("checkstyle:ForbiddenMethod")
   public static <K, V> Set<K> toKeySet(NonBlockingStore<K, V> nonBlockingStore, IntSet segments,
         Predicate<? super K> filter) {
      return Flowable.fromPublisher(nonBlockingStore.publishKeys(segments, filter))
            .collect(Collectors.toSet())
            .blockingGet();
   }

   public static <K, V> Set<K> toKeySet(AdvancedCacheLoader<K, V> acl, Predicate<? super K> filter) {
      if (acl == null)
         return Collections.emptySet();
      return singleToValue(Flowable.fromPublisher(acl.publishKeys(filter))
            .collectInto(new HashSet<>(), Set::add));
   }

   public static <K, V> Set<InternalCacheEntry<K, V>> toEntrySet(AdvancedCacheLoader<K, V> acl, Predicate<? super K> filter, final InternalEntryFactory ief) {
      if (acl == null)
         return Collections.emptySet();
      return singleToValue(Flowable.fromPublisher(acl.entryPublisher(filter, true, true))
            .map(me -> ief.create(me.getKey(), me.getValue(), me.getMetadata()))
            .collectInto(new HashSet<>(), Set::add));
   }

   /**
    * @deprecated since 9.4 This method references PersistenceManager, which isn't a public class
    */
   @Deprecated
   public static <K, V> InternalCacheEntry<K,V> loadAndStoreInDataContainer(DataContainer<K, V> dataContainer,
         final PersistenceManager persistenceManager, K key, final InvocationContext ctx, final TimeService timeService,
         final AtomicReference<Boolean> isLoaded) {
      return org.infinispan.persistence.internal.PersistenceUtil.loadAndStoreInDataContainer(dataContainer,
            persistenceManager, key, ctx, timeService, isLoaded);
   }

   /**
    * @deprecated since 9.4 This method references PersistenceManager, which isn't a public class
    */
   @Deprecated
   public static <K, V> InternalCacheEntry<K,V> loadAndStoreInDataContainer(DataContainer<K, V> dataContainer, int segment,
         final PersistenceManager persistenceManager, K key, final InvocationContext ctx, final TimeService timeService,
                                                         final AtomicReference<Boolean> isLoaded) {
      return org.infinispan.persistence.internal.PersistenceUtil.loadAndStoreInDataContainer(dataContainer, segment,
            persistenceManager, key, ctx, timeService, isLoaded);
   }

   /**
    * @deprecated since 9.4 This method references PersistenceManager, which isn't a public class
    */
   @Deprecated
   public static <K, V> InternalCacheEntry<K,V> loadAndComputeInDataContainer(DataContainer<K, V> dataContainer,
         int segment, final PersistenceManager persistenceManager, K key, final InvocationContext ctx,
         final TimeService timeService, DataContainer.ComputeAction<K, V> action) {
      return org.infinispan.persistence.internal.PersistenceUtil.loadAndComputeInDataContainer(dataContainer, segment,
            persistenceManager, key, ctx, timeService, action);
   }

   /**
    * @deprecated since 9.4 This method references PersistenceManager, which isn't a public class
    */
   @Deprecated
   public static <K, V> MarshallableEntry<K, V> loadAndCheckExpiration(PersistenceManager persistenceManager, Object key,
                                                                       InvocationContext context, TimeService timeService) {
      return org.infinispan.persistence.internal.PersistenceUtil.loadAndCheckExpiration(persistenceManager, key,
            SEGMENT_NOT_PROVIDED, context);
   }

   public static <K, V> InternalCacheEntry<K, V> convert(MarshallableEntry<K, V> loaded, InternalEntryFactory factory) {
      return org.infinispan.persistence.internal.PersistenceUtil.convert(loaded, factory);
   }

   /**
    * Will create a publisher that parallelizes each publisher returned from the <b>publisherFunction</b> by executing
    * them on the executor as needed.
    * <p>
    * Note that returned publisher will be publishing entries from the invocation of the executor. Thus any subscription
    * will not block the thread it was invoked on, unless explicitly configured to do so.
    * @param segments segments to parallelize across
    * @param executor the executor execute parallelized operations on
    * @param publisherFunction function that creates a different publisher for each segment
    * @param <R> the returned value
    * @return a publisher that
    */
   public static <R> Publisher<R> parallelizePublisher(IntSet segments, Executor executor,
         IntFunction<Publisher<R>> publisherFunction) {
      return org.infinispan.persistence.internal.PersistenceUtil.parallelizePublisher(segments, Schedulers.from(executor),
            publisherFunction);
   }

   /**
    * Replace unwanted characters from cache names so they can be used as filenames
    */
   public static String sanitizedCacheName(String cacheName) {
      return cacheName.replaceAll("[^a-zA-Z0-9-_\\.]", "_");
   }

   public static Path getQualifiedLocation(GlobalConfiguration globalConfiguration, String location, String cacheName, String qualifier) {
      Path persistentLocation = getLocation(globalConfiguration, location);
      return persistentLocation.resolve(Paths.get(sanitizedCacheName(cacheName), qualifier));
   }

   public static Path getLocation(GlobalConfiguration globalConfiguration, String location) {
      GlobalStateConfiguration globalState = globalConfiguration.globalState();
      Path persistentLocation = Paths.get(globalState.persistentLocation());
      if (location == null) {
          if (!globalState.enabled()) {
             // Should never be reached as store builders should ensure that the locations are not null during validation.
             throw PERSISTENCE.storeLocationRequired();
          }
          return persistentLocation;
      }

      Path path = Paths.get(location);
      if (!globalState.enabled()) {
          return path;
      }
      if (path.isAbsolute()) {
         // Ensure that the path lives under the global persistent location
         if (path.startsWith(persistentLocation)) {
            return path;
         } else {
            throw PERSISTENCE.forbiddenStoreLocation(path, persistentLocation);
         }
      }
      return persistentLocation.resolve(path);
   }

   public static void validateGlobalStateStoreLocation(GlobalConfiguration globalConfiguration, String storeType, Attribute<?>... attributes) {
      if (!globalConfiguration.globalState().enabled()) {
         for (Attribute<?> attr : attributes)
            if (attr.isNull())
               throw Log.CONFIG.storeLocationRequired(storeType, attr.name());
      }
   }
}
