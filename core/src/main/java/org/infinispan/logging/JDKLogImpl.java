package org.infinispan.logging;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Logger that delivers messages to a JDK logger
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class JDKLogImpl extends AbstractLogImpl {

   private final Logger logger;

   public JDKLogImpl(String category) {
      logger = Logger.getLogger(category);
   }

   private void log(Level level, Object object, Throwable ex) {
      if (logger.isLoggable(level)) {
         Throwable dummyException = new Throwable();
         StackTraceElement locations[] = dummyException.getStackTrace();
         String className = "unknown";
         String methodName = "unknown";
         int depth = 2;
         if (locations != null && locations.length > depth) {
            StackTraceElement caller = locations[depth];
            className = caller.getClassName();
            methodName = caller.getMethodName();
         }
         if (ex == null) {
            logger.logp(level, className, methodName, String.valueOf(object));
         } else {
            logger.logp(level, className, methodName, String.valueOf(object), ex);
         }
      }
   }

   public void trace(Object message) {
      log(Level.FINER, message, null);
   }

   public void debug(Object message) {
      log(Level.FINE, message, null);
   }

   public void info(Object message) {
      log(Level.INFO, message, null);
   }

   public void warn(Object message) {
      log(Level.WARNING, message, null);
   }

   public void error(Object message) {
      log(Level.SEVERE, message, null);
   }

   public void fatal(Object message) {
      log(Level.SEVERE, message, null);
   }

   public void trace(Object message, Throwable t) {
      log(Level.FINER, message, t);
   }

   public void debug(Object message, Throwable t) {
      log(Level.FINE, message, t);
   }

   public void info(Object message, Throwable t) {
      log(Level.INFO, message, t);
   }

   public void warn(Object message, Throwable t) {
      log(Level.WARNING, message, t);
   }

   public void error(Object message, Throwable t) {
      log(Level.SEVERE, message, t);
   }

   public void fatal(Object message, Throwable t) {
      log(Level.SEVERE, message, t);
   }

   public boolean isTraceEnabled() {
      return logger.isLoggable(Level.FINER);
   }

   public boolean isDebugEnabled() {
      return logger.isLoggable(Level.FINE);
   }

   public boolean isInfoEnabled() {
      return logger.isLoggable(Level.INFO);
   }

   public boolean isWarnEnabled() {
      return logger.isLoggable(Level.WARNING);
   }

   public boolean isErrorEnabled() {
      return logger.isLoggable(Level.SEVERE);
   }

   public boolean isFatalEnabled() {
      return logger.isLoggable(Level.SEVERE);
   }
}
