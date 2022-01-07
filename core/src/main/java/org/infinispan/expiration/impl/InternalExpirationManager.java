package org.infinispan.expiration.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.persistence.spi.MarshallableEntry;

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
    * This is to be invoked when a store entry expires.  This method may attempt to lock this key to preserve atomicity.
    * <p>
    * Note this method doesn't currently take a {@link InternalCacheEntry} and this is due to a limitation in the
    * cache store API.  This may cause some values to be removed if they were updated at the same time.
    * @param key the key of the expired entry
    * This method will be renamed to handleInStoreExpiration when the method can be removed from {@link ExpirationManager}
    */
   CompletionStage<Void> handleInStoreExpirationInternal(K key);

   /**
    * This is to be invoked when a store entry expires and the value and/or metadata is available to be used.
    *
    * @param marshalledEntry the entry that can be unmarshalled as needed
    * This method will be renamed to handleInStoreExpiration when the method can be removed from {@link ExpirationManager}
    */
   CompletionStage<Void> handleInStoreExpirationInternal(MarshallableEntry<K, V> marshalledEntry);

   /**
    * Handles processing for an entry that may be expired. This will remove the entry if it is expired, otherwise may
    * touch if it uses max idle.
    * @param entry entry that may be expired
    * @param segment the segment of the entry
    * @param isWrite whether the command that saw the expired value was a write or not
    * @return a stage that will complete with {@code true} if the entry was expired and {@code false} otherwise
    */
   CompletionStage<Boolean> handlePossibleExpiration(InternalCacheEntry<K, V> entry, int segment, boolean isWrite);

   /**
    * Adds an {@link ExpirationConsumer} to be invoked when an entry is expired.
    * <p>
    * It exposes the {@link PrivateMetadata}
    *
    * @param consumer The instance to invoke.
    */
   void addInternalListener(ExpirationConsumer<K, V> consumer);

   /**
    * Removes a previous registered {@link ExpirationConsumer}.
    *
    * @param listener The instance to remove.
    */
   void removeInternalListener(Object listener);

   interface ExpirationConsumer<T, U> {
      /**
       * Invoked when an entry is expired.
       *
       * @param key             The key.
       * @param value           The value.
       * @param metadata        The {@link Metadata}.
       * @param privateMetadata The {@link PrivateMetadata}.
       */
      void expired(T key, U value, Metadata metadata, PrivateMetadata privateMetadata);
   }
}
