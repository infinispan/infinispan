package org.infinispan.expiration;

import org.infinispan.configuration.cache.ExpirationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.core.MarshalledEntry;

import net.jcip.annotations.ThreadSafe;

/**
 * Central component that deals with expiration of cache entries.
 * <p />
 * Typically, {@link #processExpiration()} is called periodically by the expiration thread (which can be configured using
 * {@link ExpirationConfigurationBuilder#wakeUpInterval(long)} and {@link GlobalConfigurationBuilder#expirationThreadPool()}).
 * <p />
 * If the expiration thread is disabled - by setting {@link ExpirationConfigurationBuilder#wakeUpInterval(long)} to <tt>0</tt> -
 * then this method could be called directly, perhaps by any other maintenance thread that runs periodically in the application.
 * <p />
 * @author William Burns
 * @since 7.2
 */
@ThreadSafe
@Scope(Scopes.NAMED_CACHE)
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
    */
   void handleInMemoryExpiration(InternalCacheEntry<K, V> entry, long currentTime);

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
    * This is to be invoked with a when a write is known to occur to prevent expiration from happening.  This way we
    * won't have a swarm of remote calls required.
    * @param key the key to use
    */
   void registerWriteIncoming(K key);

   /**
    * This should always be invoked after registering write but after performing any operations required.
    * @param key the key to use
    */
   void unregisterWrite(K key);
}
