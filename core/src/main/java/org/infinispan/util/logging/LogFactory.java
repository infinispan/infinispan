package org.infinispan.util.logging;

import org.jboss.logging.Logger;

/**
 * Factory that creates {@link Log} instances.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class LogFactory {
   public static final String LOG_ROOT = "org.infinispan.";

   public static Log CLUSTER = Logger.getMessageLogger(Log.class, LOG_ROOT + "CLUSTER");

   public static Log getLog(Class<?> clazz) {
      return Logger.getMessageLogger(Log.class, clazz.getName());
   }

   public static <T> T getLog(Class<?> clazz, Class<T> logClass) {
      return Logger.getMessageLogger(logClass, clazz.getName());
   }

   public static Logger getLogger(String category) {
      return Logger.getLogger(LOG_ROOT + category);
   }

   public static void pushNDC(String cacheName, boolean isTrace) {
      // Disabled NDC usage until https://issues.jboss.org/browse/JBLOGGING-106 is fixed or
      // we introduce Log4j2 https://issues.jboss.org/browse/ISPN-3076
      // See https://issues.jboss.org/browse/ISPN-4498 for more details
//      if (isTrace)
//         NDC.push(cacheName);
   }

   public static void popNDC(boolean isTrace) {
      // Disabled NDC usage until https://issues.jboss.org/browse/JBLOGGING-106 is fixed or
      // we introduce Log4j2 https://issues.jboss.org/browse/ISPN-3076
      // See https://issues.jboss.org/browse/ISPN-4498 for more details
//      if (isTrace)
//         NDC.pop();
   }

}
