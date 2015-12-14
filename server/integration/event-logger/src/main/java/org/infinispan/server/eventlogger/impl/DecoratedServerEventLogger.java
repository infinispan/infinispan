package org.infinispan.server.eventlogger.impl;

import java.util.Optional;

import org.infinispan.util.logging.events.EventLogLevel;
import org.infinispan.util.logging.events.EventLogger;

/**
 * DecoratedServerEventLogger. Provides a way to decorate an EventLog with additional information.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
class DecoratedServerEventLogger implements EventLogger {
   private ServerEventLogger delegate;
   private Optional<String> detail;
   private Optional<String> context;
   private Optional<String> scope;
   private Optional<String> who;

   DecoratedServerEventLogger(ServerEventLogger delegate) {
      this.delegate = delegate;
   }

   @Override
   public void log(EventLogLevel level, String message) {
      delegate
            .log(new ServerEventImpl(level, delegate.getTimeService().instant(), message, detail, context, who, scope));
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

}
