package org.infinispan.eviction;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.factories.annotations.NonVolatile;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Central component that deals with eviction of cache entries.
 * <p />
 * Typically, {@link #processEviction()} is called periodically by the eviction thread (which can be configured using
 * {@link org.infinispan.config.Configuration#setEvictionWakeUpInterval(long)} and {@link org.infinispan.config.GlobalConfiguration#setEvictionScheduledExecutorFactoryClass(String)}).
 * <p />
 * If the eviction thread is disabled - by setting {@link org.infinispan.config.Configuration#setEvictionWakeUpInterval (long)} to <tt>0</tt> -
 * then this method could be called directly, perhaps by any other maintenance thread that runs periodically in the application.
 * <p />
 * Note that this method is a no-op if the eviction strategy configured is {@link org.infinispan.eviction.EvictionStrategy#NONE}.
 * <p />
 * @author Manik Surtani
 * @since 4.0
 */
@ThreadSafe
@NonVolatile
@Scope(Scopes.NAMED_CACHE)
public interface EvictionManager {

   /**
    * Processes the eviction event queue.
    */
   void processEviction();

   /**
    * @return true if eviction is enabled, false otherwise
    */
   boolean isEnabled();
}
