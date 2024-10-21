package org.infinispan.commons.jdk21.logging;

import org.jboss.logging.Logger;

/**
 * Factory that creates {@link Jdk21Log} instances.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class Jdk21LogFactory {

   public static Jdk21Log getLog(Class<?> clazz) {
      return Logger.getMessageLogger(Jdk21Log.class, clazz.getName());
   }

   public static <T> T getLog(Class<?> clazz, Class<T> logClass) {
      return Logger.getMessageLogger(logClass, clazz.getName());
   }
}
