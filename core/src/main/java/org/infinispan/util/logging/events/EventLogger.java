package org.infinispan.util.logging.events;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.Principal;
import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.security.Security;

/**
 * EventLogger provides an interface for logging event messages.
 * @author Tristan Tarrant
 * @since 8.2
 */
public interface EventLogger {
   void log(EventLogLevel level, String message);

   default void info(String message) {
      log(EventLogLevel.INFO, message);
   }

   default void warn(String message) {
      log(EventLogLevel.WARN, message);
   }

   default void error(String message) {
      log(EventLogLevel.ERROR, message);
   }

   default void fatal(String message) {
      log(EventLogLevel.FATAL, message);
   }

   default EventLogger scope(String scope) {
      return this;
   }

   default EventLogger context(Cache<?, ?> cache) {
      return context(cache.getName());
   }

   default EventLogger context(String context) {
      return this;
   }

   default EventLogger detail(String detail) {
      return this;
   }

   default EventLogger detail(Throwable t) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      pw.println(t.getLocalizedMessage());
      t.printStackTrace(pw);
      return detail(sw.toString());
   }

   default EventLogger who(Subject subject) {
      if (subject != null) {
         return this.who(Security.getSubjectUserPrincipal(subject));
      } else {
         return this;
      }
   }

   default EventLogger who(Principal principal) {
      if (principal != null) {
         return this.who(principal.getName());
      } else {
         return this;
      }
   }

   default EventLogger who(String s) {
      return this;
   }
}
