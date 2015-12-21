package org.infinispan.util.logging.events;

import org.infinispan.util.logging.events.impl.NullEventLogger;

/**
 * EventLogManager.
 *
 * This is the entry point to the event logger.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
public class EventLogManager {
   static EventLogger logger = new NullEventLogger();

   /**
    * Retrieves the event logger
    */
   public static EventLogger getEventLogger() {
      return logger;
   }

   public static EventLogger replaceEventLogger(EventLogger newLogger) {
      EventLogger oldLogger = logger;
      logger = newLogger;
      return oldLogger;
   }
}
