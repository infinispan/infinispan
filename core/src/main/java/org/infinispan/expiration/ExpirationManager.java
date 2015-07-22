package org.infinispan.expiration;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.configuration.cache.ExpirationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

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
    * This should be invoked passing in an entry that is now expired.  Note this method assumes to not have the
    * lock for this key and will attempt to acquire it.
    * @param entry entry that is now expired
    */
   void handleInMemoryExpiration(InternalCacheEntry<K, V> entry);

   void handleInStoreExpiration(K key);
}
