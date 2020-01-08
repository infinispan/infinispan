package org.infinispan.expiration.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.util.concurrent.CompletionStages;

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
    * <p>
    * If <b>hasLock</b> is true, this method assumes that the caller has the lock for the key and it must allow the
    * expiration to occur, ie. returned CompletableFuture has completed, before the lock is released. Failure to do
    * so may cause inconsistency in data.
    * @param entry the entry that has expired
    * @param currentTime the current time when it expired
    * @param isWrite if the expiration was found during a write operation
    * @return if this entry actually expired or not
    */
   CompletableFuture<Boolean> entryExpiredInMemory(InternalCacheEntry<K, V> entry, long currentTime, boolean isWrite);

   /**
    * This method is very similar to {@link #entryExpiredInMemory(InternalCacheEntry, long, boolean)} except that it does the
    * bare minimum when an entry expired to guarantee if the entry is valid or not. This is important to reduce time
    * spent per entry when iterating. This method may not actually remove the entry and may just return immediately
    * if it is safe to do so.
    * @param entry the entry that has expired
    * @param currentTime the current time when it expired
    * @return if this entry actually expired or not
    */
   boolean entryExpiredInMemoryFromIteration(InternalCacheEntry<K, V> entry, long currentTime);

   /**
    * This is to be invoked when a store entry expires.  This method may attempt to lock this key to preserve atomicity.
    * <p>
    * Note this method doesn't currently take a {@link InternalCacheEntry} and this is due to a limitation in the
    * cache store API.  This may cause some values to be removed if they were updated at the same time.
    * @param key the key of the expired entry
    * This method will be renamed to handleInStoreExpiration when the method can be removed from {@link ExpirationManager}
    */
   CompletionStage<Void> handleInStoreExpirationInternal(K key);

   @Override
   default void handleInStoreExpiration(K key) {
      CompletionStages.join(handleInStoreExpirationInternal(key));
   }

   /**
    * This is to be invoked when a store entry expires and the value and/or metadata is available to be used.  This
    * method is preferred over {@link ExpirationManager#handleInStoreExpiration(Object)} as it allows for more
    * specific expiration to possibly occur.
    * @param marshalledEntry the entry that can be unmarshalled as needed
    * This method will be renamed to handleInStoreExpiration when the method can be removed from {@link ExpirationManager}
    */
   CompletionStage<Void> handleInStoreExpirationInternal(MarshalledEntry<K, V> marshalledEntry);

   default void handleInStoreExpiration(MarshalledEntry<K, V> marshalledEntry) {
      CompletionStages.join(handleInStoreExpirationInternal(marshalledEntry));
   }

   /**
    * Handles processing for an entry that may be expired. This will remove the entry if it is expired, otherwise may
    * touch if it uses max idle. The return stage will contain whether the entry was actually expired or not
    * @param entry entry that may be expired
    * @param segment the segment of the entry
    * @param isWrite whether the command that saw the expired value was a write or not
    * @return a stage that when complete will return if the entry was expired
    */
   CompletionStage<Boolean> handlePossibleExpiration(InternalCacheEntry<K, V> entry, int segment, boolean isWrite);
}
