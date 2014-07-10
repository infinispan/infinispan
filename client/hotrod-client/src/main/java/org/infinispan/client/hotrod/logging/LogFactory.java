package org.infinispan.client.hotrod.logging;

import org.jboss.logging.Logger;
import org.jboss.logging.NDC;

/**
 * Factory that creates {@link Log} instances.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class LogFactory {

   public static Log getLog(Class<?> clazz) {
      return Logger.getMessageLogger(Log.class, clazz.getName());
   }

   public static <T> T getLog(Class<?> clazz, Class<T> logClass) {
      return Logger.getMessageLogger(logClass, clazz.getName());
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
