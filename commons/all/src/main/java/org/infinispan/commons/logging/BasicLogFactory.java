package org.infinispan.commons.logging;

import java.lang.invoke.MethodHandles;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;

/**
 * Factory that creates {@link Log} instances.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class BasicLogFactory {

   public static BasicLogger getLog(Class<?> clazz) {
      return Logger.getLogger(clazz.getName());
   }

   public static <T> T getLog(Class<?> clazz, Class<T> logClass) {
      return Logger.getMessageLogger(MethodHandles.lookup(), logClass, clazz.getName());
   }

}
