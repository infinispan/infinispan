package org.infinispan.util.logging;

import org.infinispan.util.ByteString;
import org.jboss.logging.Logger;
import org.jboss.logging.NDC;

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

   public static <T> T getLog(String category, Class<T> logClass) {
      return Logger.getMessageLogger(logClass, LOG_ROOT + category);
   }

   public static Logger getLogger(String category) {
      return Logger.getLogger(LOG_ROOT + category);
   }

   public static void pushNDC(String cacheName, boolean isTrace) {
      if (isTrace)
         NDC.push(cacheName);
   }

   public static void pushNDC(ByteString cacheName, boolean isTrace) {
      if (isTrace)
         NDC.push(cacheName.toString());
   }

   public static void popNDC(boolean isTrace) {
      if (isTrace)
         NDC.pop();
   }

}
