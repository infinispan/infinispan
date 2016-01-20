package org.infinispan.server.eventlogger;

import static org.infinispan.util.logging.events.Messages.MESSAGES;

import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;
import org.infinispan.util.logging.events.impl.DecoratedEventLogger;

/**
 * DecoratedServerEventLogger. Provides a way to decorate an EventLog with additional information.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
class DecoratedServerEventLogger extends DecoratedEventLogger {
   private ServerEventLogger delegate;

   DecoratedServerEventLogger(ServerEventLogger delegate) {
      super(delegate);
      this.delegate = delegate;
   }

   @Override
   public void log(EventLogLevel level, EventLogCategory category, String message) {
      StringBuilder sb = new StringBuilder();
      context.ifPresent(c -> sb.append(MESSAGES.eventLogContext(c)));
      scope.ifPresent(s -> sb.append(MESSAGES.eventLogContext(s)));
      who.ifPresent(w -> sb.append(MESSAGES.eventLogWho(w)));
      // We don't include detail in this implementation
      sb.append(message);
      delegate.textLog(level, category, sb.toString());
      delegate.eventLog(new ServerEventImpl(level, category, delegate.getTimeService().instant(), message, detail, context, who, scope));
   }
}
