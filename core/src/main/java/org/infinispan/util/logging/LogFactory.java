package org.infinispan.util.logging;

import java.lang.invoke.MethodHandles;

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

   public static Log getLog(Class<?> clazz) {
      return Logger.getMessageLogger(MethodHandles.lookup(), Log.class, clazz.getName());
   }

   public static <T> T getLog(Class<?> clazz, Class<T> logClass) {
      return Logger.getMessageLogger(MethodHandles.lookup(), logClass, clazz.getName());
   }

   public static <T> T getLog(String category, Class<T> logClass) {
      return Logger.getMessageLogger(MethodHandles.lookup(), logClass, Log.LOG_ROOT + category);
   }

   public static Logger getLogger(String category) {
      return Logger.getLogger(Log.LOG_ROOT + category);
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
