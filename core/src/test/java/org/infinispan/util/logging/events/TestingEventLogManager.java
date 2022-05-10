package org.infinispan.util.logging.events;

import org.infinispan.commons.time.DefaultTimeService;
import org.infinispan.util.logging.events.impl.BasicEventLogger;
import org.infinispan.util.logging.events.impl.EventLoggerNotifierImpl;

public class TestingEventLogManager implements EventLogManager {
   private EventLogger logger = new BasicEventLogger(new EventLoggerNotifierImpl(), DefaultTimeService.INSTANCE);

   @Override
   public EventLogger getEventLogger() {
      return logger;
   }

   @Override
   public EventLogger replaceEventLogger(EventLogger newLogger) {
      EventLogger oldLogger = logger;
      logger = newLogger;
      return oldLogger;
   }
}
