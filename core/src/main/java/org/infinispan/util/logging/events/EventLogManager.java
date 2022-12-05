package org.infinispan.util.logging.events;

import static java.util.Objects.requireNonNull;

import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.actions.SecurityActions;

/**
 * EventLogManager.
 *
 * This is the entry point to the event logger.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
public interface EventLogManager {

   /**
    * @return the event logger for the given {@link EmbeddedCacheManager}
    * @throws IllegalLifecycleStateException if the cache manager is not running
    */
   static EventLogger getEventLogger(EmbeddedCacheManager cacheManager) {
      requireNonNull(cacheManager, "EmbeddedCacheManager can't be null.");

      if (cacheManager.getStatus() != ComponentStatus.RUNNING)
         throw new IllegalLifecycleStateException();

      return SecurityActions.getGlobalComponentRegistry(cacheManager)
                         .getComponent(BasicComponentRegistry.class)
                         .getComponent(EventLogManager.class)
                         .running().getEventLogger();
   }

   /**
    * @return the event logger
    */
   EventLogger getEventLogger();

   /**
    * Replaces the event logger with the provided one.
    *
    * @return the previous logger
    */
   EventLogger replaceEventLogger(EventLogger newLogger);
}
