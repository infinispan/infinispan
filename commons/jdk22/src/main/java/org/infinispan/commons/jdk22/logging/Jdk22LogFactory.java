package org.infinispan.commons.jdk22.logging;

import org.jboss.logging.Logger;

/**
 * Factory that creates {@link Jdk22Log} instances.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class Jdk22LogFactory {

   public static Jdk22Log getLog(Class<?> clazz) {
      return Logger.getMessageLogger(Jdk22Log.class, clazz.getName());
   }

   public static <T> T getLog(Class<?> clazz, Class<T> logClass) {
      return Logger.getMessageLogger(logClass, clazz.getName());
   }
}
