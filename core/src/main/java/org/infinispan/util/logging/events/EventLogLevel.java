package org.infinispan.util.logging.events;

import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.jboss.logging.Logger.Level;

/**
 * EventLogLevel.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
public enum EventLogLevel {

   @ProtoEnumValue(number = 1)
   INFO(Level.INFO),

   @ProtoEnumValue(number = 2)
   WARN(Level.WARN),

   @ProtoEnumValue(number = 3)
   ERROR(Level.ERROR),

   @ProtoEnumValue(number = 4)
   FATAL(Level.FATAL);

   private final Level loggerLevel;

   EventLogLevel(Level loggerLevel) {
      this.loggerLevel = loggerLevel;
   }

   public Level toLoggerLevel() {
      return loggerLevel;
   }
}
