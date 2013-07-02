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
      if (isTrace)
         NDC.push(cacheName);
   }

   public static void popNDC(boolean isTrace) {
      if (isTrace)
         NDC.pop();
   }

}
