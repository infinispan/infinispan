package org.infinispan.expiration;

import java.util.concurrent.CompletableFuture;

import org.infinispan.configuration.cache.ExpirationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.core.MarshalledEntry;

import net.jcip.annotations.ThreadSafe;

/**
 * Central component that deals with expiration of cache entries.
 * <p>
 * Typically, {@link #processExpiration()} is called periodically by the expiration thread (which can be configured using
 * {@link ExpirationConfigurationBuilder#wakeUpInterval(long)} and {@link GlobalConfigurationBuilder#expirationThreadPool()}).
 * <p>
 * If the expiration thread is disabled - by setting {@link ExpirationConfigurationBuilder#wakeUpInterval(long)} to <tt>0</tt> -
 * then this method could be called directly, perhaps by any other maintenance thread that runs periodically in the application.
 *
 * @author William Burns
 * @since 7.2
 */
@ThreadSafe
public interface ExpirationManager<K, V> {

   /**
    * Processes the expiration event queue.
    */
   void processExpiration();

   /**
    * @return true if expiration reaper thread is enabled, false otherwise
    */
   boolean isEnabled();

   /**
    * This should be invoked passing in an entry that is now expired.  This method may attempt to lock this key to
    * preserve atomicity.
    * @param entry entry that is now expired
    * @param currentTime the current time in milliseconds
    * @deprecated since 9.3 this method is not intended for external use
    */
   @Deprecated
   void handleInMemoryExpiration(InternalCacheEntry<K, V> entry, long currentTime);

   /**
    * This is to be invoked when a store entry expires.  This method may attempt to lock this key to preserve atomicity.
    * <p>
    * Note this method doesn't currently take a {@link InternalCacheEntry} and this is due to a limitation in the
    * cache store API.  This may cause some values to be removed if they were updated at the same time.
    * @param key the key of the expired entry
    * @deprecated since 9.3 this method is not intended for external use
    */
   @Deprecated
   void handleInStoreExpiration(K key);

   /**
    * This is to be invoked when a store entry expires and the value and/or metadata is available to be used.  This
    * method is preferred over {@link ExpirationManager#handleInStoreExpiration(Object)} as it allows for more
    * specific expiration to possibly occur.
    * @param marshalledEntry the entry that can be unmarshalled as needed
    * @deprecated since 9.3 this method is not intended for external use
    */
   @Deprecated
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
    * @deprecated since 9.3 this method is not intended for external use
    */
   @Deprecated
   CompletableFuture<Long> retrieveLastAccess(Object key, Object value, int segment);

   /**
    * This is to be invoked with a when a write is known to occur to prevent expiration from happening.  This way we
    * won't have a swarm of remote calls required.
    * @param key the key to use
    * @deprecated since 9.3 There is no reason for this method and is implementation specific
    */
   @Deprecated
   default void registerWriteIncoming(K key) { }

   /**
    * This should always be invoked after registering write but after performing any operations required.
    * @param key the key to use
    * @deprecated since 9.3 There is no reason for this method and is implementation specific
    */
   @Deprecated
   default void unregisterWrite(K key) { }
}
