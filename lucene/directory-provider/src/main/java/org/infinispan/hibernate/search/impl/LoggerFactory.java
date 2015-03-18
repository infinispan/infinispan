package org.infinispan.hibernate.search.impl;

import org.infinispan.hibernate.search.logging.Log;
import org.jboss.logging.Logger;

/**
 * Factory for obtaining {@link Logger} instances.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2012 Red Hat Inc.
 * @author Gunnar Morling
 */
public class LoggerFactory {

   private static final CallerProvider callerProvider = new CallerProvider();

   private LoggerFactory() {
   }

   public static Log make() {
      return Logger.getMessageLogger(Log.class, callerProvider.getCallerClass().getCanonicalName());
   }

   private static class CallerProvider extends SecurityManager {

      public Class<?> getCallerClass() {
         return getClassContext()[2];
      }
   }
}
