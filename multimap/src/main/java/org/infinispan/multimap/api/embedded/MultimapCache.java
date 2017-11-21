package org.infinispan.multimap.api.embedded;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import org.infinispan.commons.util.Experimental;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.multimap.api.BasicMultimapCache;
import org.infinispan.util.function.SerializablePredicate;

/**
 * {@inheritDoc}
 *
 * Embedded version of MultimapCache.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @see <a href="http://infinispan.org/documentation/">Infinispan documentation</a>
 * @since 9.2
 */
@Experimental
public interface MultimapCache<K, V> extends BasicMultimapCache<K, V> {

   /**
    * Retrieves a CacheEntry corresponding to a specific key in this multimap cache.
    *
    * @param key the key whose associated cache entry is to be returned
    * @return the cache entry to which the specified key is mapped, or {@link Optional#empty()} if this multimap
    * contains no mapping for the key
    * @since 9.2
    */
   CompletableFuture<Optional<CacheEntry<K, Collection<V>>>> getEntry(K key);

   /**
    * Asynchronous method. Removes every value that match the {@link Predicate}.
    * <p>
    * This method <b>is blocking</b> used in a explicit transaction context.
    *
    * @param p the predicate to be tested on every value in this multimap cache
    * @return {@link CompletableFuture} containing a {@link Void}
    * @since 9.2
    */
   CompletableFuture<Void> remove(Predicate<? super V> p);

   /**
    * Overloaded method of {@link MultimapCache#remove(Predicate)} with {@link SerializablePredicate}. The compiler will
    * pick up this method and make the given predicate {@link java.io.Serializable}.
    *
    * @param p the predicate to be tested on every value in this multimap cache
    * @return {@link CompletableFuture} containing a {@link Void}
    * @since 9.2
    */
   default CompletableFuture<Void> remove(SerializablePredicate<? super V> p) {
      return this.remove((Predicate) p);
   }

}
