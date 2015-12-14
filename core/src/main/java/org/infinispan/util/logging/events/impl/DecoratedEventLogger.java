package org.infinispan.util.logging.events.impl;

import java.util.List;
import java.util.Optional;

import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;
import org.infinispan.util.logging.events.EventLogger;

import static org.infinispan.util.logging.events.Messages.MESSAGES;

/**
 * DecoratedEventLogger. Provides a way to decorate an EventLog with additional information.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
public class DecoratedEventLogger implements EventLogger {
   private EventLogger delegate;
   protected Optional<String> detail = Optional.empty();
   protected Optional<String> context = Optional.empty();
   protected Optional<String> scope = Optional.empty();
   protected Optional<String> who = Optional.empty();

   protected DecoratedEventLogger(EventLogger delegate) {
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
      delegate.log(level, category, sb.toString());
   }

   @Override
   public EventLogger who(String who) {
      this.who = Optional.of(who);
      return this;
   }

   @Override
   public EventLogger scope(String scope) {
      this.scope = Optional.of(scope);
      return this;
   }

   @Override
   public EventLogger context(String context) {
      this.context = Optional.of(context);
      return this;
   }

   @Override
   public EventLogger detail(String detail) {
      this.detail = Optional.ofNullable(detail);
      return this;
   }

   @Override
   public List<EventLog> getEvents(int start, int count) {
      return delegate.getEvents(start, count);
   }

}
