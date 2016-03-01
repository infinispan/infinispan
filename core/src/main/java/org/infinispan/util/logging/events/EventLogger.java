package org.infinispan.util.logging.events;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.Principal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.Security;

/**
 * EventLogger provides an interface for logging event messages.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
public interface EventLogger {
   /**
    * Logs a message to the event log with the specified level
    *
    * @param level
    *           the severity level of the event
    * @param message
    *           the message to log
    */
   void log(EventLogLevel level, EventLogCategory category, String message);

   /**
    * Logs a message to the event log using the {@link EventLogLevel#INFO} severity
    *
    * @param message
    *           the message to log
    */
   default void info(EventLogCategory category, String message) {
      log(EventLogLevel.INFO, category, message);
   }

   /**
    * Logs a message to the event log using the {@link EventLogLevel#WARN} severity
    *
    * @param message
    *           the message to log
    */
   default void warn(EventLogCategory category, String message) {
      log(EventLogLevel.WARN, category, message);
   }

   /**
    * Logs a message to the event log using the {@link EventLogLevel#ERROR} severity
    *
    * @param message
    *           the message to log
    */
   default void error(EventLogCategory category, String message) {
      log(EventLogLevel.ERROR, category, message);
   }

   /**
    * Logs a message to the event log using the {@link EventLogLevel#FATAL} severity
    *
    * @param message
    *           the message to log
    */
   default void fatal(EventLogCategory category, String message) {
      log(EventLogLevel.FATAL, category, message);
   }

   /**
    * Sets the scope of this event log, e.g. a node address. This should be used for events which
    * reference a single node in the cluster
    *
    * @param scope a scope
    * @return the event logger
    */
   default EventLogger scope(String scope) {
      return this;
   }

   /**
    * Sets a node address as the scope of this event log
    *
    * @param scope the address of the node
    * @return the event logger
    */
   default EventLogger scope(Address scope) { return this; }

   /**
    * Sets a cache as context of this event log. The name of the cache will be used to indicate the
    * context.
    *
    * @param cache
    *           the cache to set as context
    * @return the event logger
    */
   default EventLogger context(Cache<?, ?> cache) {
      return context(cache.getName());
   }

   /**
    * Sets a context of this event log.
    *
    * @param context
    *           the name of the context
    * @return the event logger
    */
   default EventLogger context(String context) {
      return this;
   }

   /**
    * Sets a detail for this event log which could include additional information.
    *
    * @param detail
    *           the event log detail
    * @return the event logger
    */
   default EventLogger detail(String detail) {
      return this;
   }

   /**
    * Sets a throwable to include as detail for this event. Both the localized message of the
    * Throwable as well as its stack trace will be recorded as the event's detail
    *
    * @param detail
    *           a throwable
    * @return the event logger
    */
   default EventLogger detail(Throwable t) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      pw.println(t.getLocalizedMessage());
      t.printStackTrace(pw);
      return detail(sw.toString());
   }

   /**
    * Sets a security subject for this event log. The name of the main user principal of the subject
    * will be recorded in the log.
    *
    * @param subject
    *           the security subject
    * @return the event logger
    */
   default EventLogger who(Subject subject) {
      if (subject != null) {
         return this.who(Security.getSubjectUserPrincipal(subject));
      } else {
         return this;
      }
   }

   /**
    * Sets a security principal for this event log. The name of the principal will be recorded in
    * the log.
    *
    * @param principal
    *           the security principal
    * @return the event logger
    */
   default EventLogger who(Principal principal) {
      if (principal != null) {
         return this.who(principal.getName());
      } else {
         return this;
      }
   }

   /**
    * Sets a security name for this event log.
    *
    * @param s
    *           the security name
    * @return the event logger
    */
   default EventLogger who(String s) {
      return this;
   }

   /**
    * Retrieves the event logs from the cluster within the specified range
    *
    * @param start
    *           the instant from which to retrieve the logs
    * @param count
    *           the number of logs to retrieve
    * @param category
    *           an optional category filter
    * @param level
    *           an optional level filter
    * @return a list of {@link EventLog}s
    */
   List<EventLog> getEvents(Instant start, int count, Optional<EventLogCategory> category, Optional<EventLogLevel> level);
}
