package org.infinispan.util.logging.events.impl;

import org.infinispan.util.logging.events.EventLogLevel;
import org.infinispan.util.logging.events.EventLogger;

/**
 * NullEventLogger. An event logger which doesn't do anything
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
public class NullEventLogger implements EventLogger {
   @Override
   public void log(EventLogLevel level, String message) {
      // Snore
   }
}
