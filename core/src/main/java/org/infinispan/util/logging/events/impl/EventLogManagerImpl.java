package org.infinispan.util.logging.events.impl;

import org.infinispan.commons.time.TimeService;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;
import org.infinispan.util.logging.events.EventLoggerNotifier;

/**
 * EventLogManagerImpl. The default implementation of the EventLogManager.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
@Scope(Scopes.GLOBAL)
public class EventLogManagerImpl implements EventLogManager {

   @Inject protected EventLoggerNotifier notifier;
   @Inject protected TimeService timeService;

   private EventLogger logger;

   @Start
   public void start() {
      this.logger = new BasicEventLogger(notifier, timeService);
   }

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
