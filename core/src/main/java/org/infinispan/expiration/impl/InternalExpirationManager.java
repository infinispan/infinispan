package org.infinispan.expiration.impl;

import java.util.concurrent.CompletableFuture;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.core.MarshalledEntry;

/**
 * Interface describing the internal operations for the the ExpirationManager.
 * @author wburns
 * @since 9.3
 */
@Scope(Scopes.NAMED_CACHE)
public interface InternalExpirationManager<K, V> extends ExpirationManager<K, V> {
   /**
    * This should be invoked passing in an entry that is now expired.  This method may attempt to lock this key to
    * preserve atomicity. This method should be invoked when an entry was read via get but found to be expired.
    * <p>
    * This method returns <b>true</b> if the entry was removed due to expiration or <b>false</b> if the entry was
    * not removed due to expiration
    * @param entry the entry that has expired
    * @param currentTime the current time when it expired
    * @return if this entry actually expired or not
    */
   CompletableFuture<Boolean> entryExpiredInMemory(InternalCacheEntry<K, V> entry, long currentTime);

   /**
    * This method is very similar to {@link #entryExpiredInMemory(InternalCacheEntry, long)} except that it does the
    * bare minimum when an entry expired to guarantee if the entry is valid or not. This is important to reduce time
    * spent per entry when iterating. This method may not actually remove the entry and may just return immediately
    * if it is safe to do so.
    * @param entry the entry that has expired
    * @param currentTime the current time when it expired
    * @return if this entry actually expired or not
    */
   CompletableFuture<Boolean> entryExpiredInMemoryFromIteration(InternalCacheEntry<K, V> entry, long currentTime);

   /**
    * This is to be invoked when a store entry expires.  This method may attempt to lock this key to preserve atomicity.
    * <p>
    * Note this method doesn't currently take a {@link InternalCacheEntry} and this is due to a limitation in the
    * cache store API.  This may cause some values to be removed if they were updated at the same time.
    * @param key the key of the expired entry
    */
   void handleInStoreExpiration(K key);

   /**
    * This is to be invoked when a store entry expires and the value and/or metadata is available to be used.  This
    * method is preferred over {@link ExpirationManager#handleInStoreExpiration(Object)} as it allows for more
    * specific expiration to possibly occur.
    * @param marshalledEntry the entry that can be unmarshalled as needed
    */
   void handleInStoreExpiration(MarshalledEntry<K, V> marshalledEntry);

   /**
    * Retrieves the last access time for the given key in the data container if it is using max idle.
    * If the entry is not in the container or it is expired it will return null.
    * If the entry is present but cannot expire via max idle, it will return -1
    * If the entry is present and can expire via max idle but hasn't it will return a number > 0
    * @param key the key to retrieve the access time for
    * @param value the value to match if desired (this can be null)
    * @param segment the segment for the given key
    * @return the last access time if available
    */
   CompletableFuture<Long> retrieveLastAccess(Object key, Object value, int segment);
}
