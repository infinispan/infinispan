package org.infinispan.util.logging.events;

import org.infinispan.manager.EmbeddedCacheManager;

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
    * @return the event logger for the given {@link CacheManager}
    */
   static EventLogger getEventLogger(EmbeddedCacheManager cacheManager) {
      EventLogManager eventLogManager = cacheManager.getGlobalComponentRegistry().getComponent(EventLogManager.class);
      return eventLogManager.getEventLogger();
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
