package org.infinispan.persistence;

import static org.infinispan.util.logging.Log.PERSISTENCE;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalStateConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.util.logging.Log;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class PersistenceUtil {

   // This method is blocking - but only invoked by tests or user code
   @SuppressWarnings("checkstyle:ForbiddenMethod")
   public static <K, V> Set<K> toKeySet(NonBlockingStore<K, V> nonBlockingStore, IntSet segments,
         Predicate<? super K> filter) {
      return Flowable.fromPublisher(nonBlockingStore.publishKeys(segments, filter))
            .collect(Collectors.toSet())
            .blockingGet();
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
