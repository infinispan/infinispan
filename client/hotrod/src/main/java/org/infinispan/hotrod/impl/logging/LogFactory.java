package org.infinispan.hotrod.impl.logging;

import org.jboss.logging.Logger;

/**
 * Factory that creates {@link Log} instances.
 *
 * @since 14.0
 */
public class LogFactory {

   public static Log getLog(Class<?> clazz) {
      return Logger.getMessageLogger(Log.class, clazz.getName());
   }

   public static <T> T getLog(Class<?> clazz, Class<T> logClass) {
      return Logger.getMessageLogger(logClass, clazz.getName());
   }

}
