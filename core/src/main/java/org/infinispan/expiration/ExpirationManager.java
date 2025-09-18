package org.infinispan.expiration;

import org.infinispan.configuration.cache.ExpirationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

import com.google.errorprone.annotations.ThreadSafe;

/**
 * Central component that deals with expiration of cache entries.
 * Typically, {@link #processExpiration()} is called periodically by the expiration thread (which can be configured using
 * {@link ExpirationConfigurationBuilder#wakeUpInterval(long)} and {@link GlobalConfigurationBuilder#expirationThreadPool()}).
 * If the expiration thread is disabled - by setting {@link ExpirationConfigurationBuilder#wakeUpInterval(long)} to <code>0</code> -
 * then this method could be called directly, perhaps by any other maintenance thread that runs periodically in the application.
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
}
