package org.infinispan.util.logging.events.impl;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.time.TimeService;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;
import org.infinispan.util.logging.events.EventLogger;
import org.infinispan.util.logging.events.EventLoggerNotifier;

/**
 * BasicEventLogger. An event logger which doesn't do anything aside from sending events to the
 * logger and notifying the listeners.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
public class BasicEventLogger implements EventLogger {

   private final EventLoggerNotifier notifier;
   private final TimeService timeService;

   public BasicEventLogger(EventLoggerNotifier notifier, TimeService timeService) {
      this.notifier = notifier;
      this.timeService = timeService;
   }

   @Override
   public EventLogger scope(String scope) {
      return new DecoratedEventLogger(this).scope(scope);
   }

   @Override
   public EventLogger context(String context) {
      return new DecoratedEventLogger(this).context(context);
   }

   @Override
   public EventLogger detail(String detail) {
      return new DecoratedEventLogger(this).detail(detail);
   }

   @Override
   public EventLogger who(String who) {
      return new DecoratedEventLogger(this).who(who);
   }

   @Override
   public void log(EventLogLevel level, EventLogCategory category, String message) {
      LogFactory.getLogger(category.toString()).log(level.toLoggerLevel(), message);
      CompletionStages.join(notifier.notifyEventLogged(new BaseEventLog(timeService.instant(), level, category, message)));
   }

   /**
    * The basic event logger doesn't collect anything.
    */
   @Override
   public List<EventLog> getEvents(Instant start, int count, Optional<EventLogCategory> category, Optional<EventLogLevel> level) {
      return Collections.emptyList();
   }

   @Override
   public CompletionStage<Void> addListenerAsync(Object listener) {
      return notifier.addListenerAsync(listener);
   }

   @Override
   public CompletionStage<Void> removeListenerAsync(Object listener) {
      return notifier.removeListenerAsync(listener);
   }

   @Override
   public Set<Object> getListeners() {
      return notifier.getListeners();
   }
}
