package org.infinispan.util.logging.events.impl;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;

/**
 * EventLogManagerImpl. The implementation of the EventLogManager.
 * By default this returns
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
@Scope(Scopes.GLOBAL)
public class EventLogManagerImpl implements EventLogManager {
   EventLogger logger = new BasicEventLogger();

   @Override
   public EventLogger replaceEventLogger(EventLogger newLogger) {
      EventLogger oldLogger = logger;
      logger = newLogger;
      return oldLogger;
   }

   @Override
   public EventLogger getEventLogger() {
      return logger;
   }
}
