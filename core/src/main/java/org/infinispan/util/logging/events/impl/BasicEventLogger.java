package org.infinispan.util.logging.events.impl;

import java.util.List;

import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;
import org.infinispan.util.logging.events.EventLogger;

/**
 * BasicEventLogger. An event logger which doesn't do anything aside from sending events to the
 * logger
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
public class BasicEventLogger implements EventLogger {
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
   }

   /**
    * The null event logger doesn't collect anything.
    */
   @Override
   public List<EventLog> getEvents(int start, int count) {
      return InfinispanCollections.emptyList();
   }
}
