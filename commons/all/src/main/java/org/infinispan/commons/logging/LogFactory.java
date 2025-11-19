package org.infinispan.commons.logging;

import java.lang.invoke.MethodHandles;

import org.jboss.logging.Logger;

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

   public static Log getLog(String category) {
      return Logger.getMessageLogger(MethodHandles.lookup(), Log.class, Log.LOG_ROOT + category);
   }
}
