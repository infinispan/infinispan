package org.infinispan.util.logging.events;

import org.jboss.logging.Logger.Level;

/**
 * EventLogLevel.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
public enum EventLogLevel {
   INFO(Level.INFO), WARN(Level.WARN), ERROR(Level.ERROR), FATAL(Level.FATAL);

   private final Level loggerLevel;

   EventLogLevel(Level loggerLevel) {
      this.loggerLevel = loggerLevel;
   }

   public Level toLoggerLevel() {
      return loggerLevel;
   }
}
