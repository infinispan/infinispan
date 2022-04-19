package org.infinispan.util.logging.events.impl;

import static org.infinispan.util.logging.events.Messages.MESSAGES;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;
import org.infinispan.util.logging.events.EventLogger;

/**
 * DecoratedEventLogger. Provides a way to decorate an EventLog with additional information.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
public class DecoratedEventLogger implements EventLogger {
   private static final String LOCAL_SCOPE = "local";
   private final EventLogger delegate;
   protected String detail;
   protected String context;
   protected String scope;
   protected String who;

   protected DecoratedEventLogger(EventLogger delegate) {
      this.delegate = delegate;
   }

   @Override
   public void log(EventLogLevel level, EventLogCategory category, String message) {
      StringBuilder sb = new StringBuilder();
      addLogsToBuilder(sb);
      // We don't include detail in this implementation
      sb.append(' ');
      sb.append(message);
      delegate.log(level, category, sb.toString());
   }

   protected void addLogsToBuilder(StringBuilder sb) {
      if (context != null) sb.append(MESSAGES.eventLogContext(context));
      if (scope != null) sb.append(MESSAGES.eventLogScope(scope));
      if (who != null) sb.append(MESSAGES.eventLogWho(who));
   }

   @Override
   public EventLogger who(String who) {
      this.who = who;
      return this;
   }

   @Override
   public EventLogger scope(String scope) {
      this.scope = scope;
      return this;
   }

   @Override
   public EventLogger scope(Address scope) {
      this.scope = scope != null ? scope.toString() : LOCAL_SCOPE;
      return this;
   }

   @Override
   public EventLogger context(String context) {
      this.context = context;
      return this;
   }

   @Override
   public EventLogger detail(String detail) {
      this.detail = detail;
      return this;
   }

   @Override
   public List<EventLog> getEvents(Instant start, int count, Optional<EventLogCategory> category, Optional<EventLogLevel> level) {
      return delegate.getEvents(start, count, category, level);
   }

   @Override
   public CompletionStage<Void> addListenerAsync(Object listener) {
      return delegate.addListenerAsync(listener);
   }

   @Override
   public CompletionStage<Void> removeListenerAsync(Object listener) {
      return delegate.removeListenerAsync(listener);
   }

   @Override
   public Set<Object> getListeners() {
      return delegate.getListeners();
   }
}
