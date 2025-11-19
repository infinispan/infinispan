package org.infinispan.client.hotrod.logging;

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
}
