package org.infinispan.expiration;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.configuration.cache.ExpirationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Central component that deals with expiration of cache entries.
 * <p />
 * Typically, {@link #processExpiration()} is called periodically by the expiration thread (which can be configured using
 * {@link ExpirationConfigurationBuilder#wakeUpInterval(Long)} and {@link GlobalConfigurationBuilder#expirationThreadPool()}).
 * <p />
 * If the expiration thread is disabled - by setting {@link ExpirationConfigurationBuilder#wakeUpInterval(Long)} to <tt>0</tt> -
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
    * @return true if expiration is enabled, false otherwise
    */
   boolean isEnabled();
}
