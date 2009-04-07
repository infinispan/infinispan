package org.infinispan.logging;

/**
 * Abstract log implementation that handles message interpolation
 *
 * @author Manik Surtani
 */
public abstract class AbstractLogImpl implements Log {
   public void trace(Object message, Object... params) {
      if (isTraceEnabled()) trace(substitute(message, params));
   }

   public void debug(Object message, Object... params) {
      if (isDebugEnabled()) debug(substitute(message, params));
   }

   public void info(Object message, Object... params) {
      if (isInfoEnabled()) info(substitute(message, params));
   }

   public void warn(Object message, Object... params) {
      if (isWarnEnabled()) warn(substitute(message, params));
   }

   public void error(Object message, Object... params) {
      if (isErrorEnabled()) error(substitute(message, params));
   }

   public void fatal(Object message, Object... params) {
      if (isFatalEnabled()) fatal(substitute(message, params));
   }

   public void trace(Object message, Throwable t, Object... params) {
      if (isTraceEnabled()) trace(substitute(message, params), t);
   }

   public void debug(Object message, Throwable t, Object... params) {
      if (isDebugEnabled()) debug(substitute(message, params), t);
   }

   public void info(Object message, Throwable t, Object... params) {
      if (isInfoEnabled()) info(substitute(message, params), t);
   }

   public void warn(Object message, Throwable t, Object... params) {
      if (isWarnEnabled()) warn(substitute(message, params), t);
   }

   public void error(Object message, Throwable t, Object... params) {
      if (isErrorEnabled()) error(substitute(message, params), t);
   }

   public void fatal(Object message, Throwable t, Object... params) {
      if (isFatalEnabled()) fatal(substitute(message, params), t);
   }

   private Object substitute(Object message, Object... params) {
      if (params.length == 0) return message;

      StringBuilder value = new StringBuilder(String.valueOf(message));
      for (int i = 0; i < params.length; i++) {
         String placeholder = "{" + i + "}";
         int phIndex;
         if ((phIndex = value.indexOf(placeholder)) > -1) {
            value = value.replace(phIndex, phIndex + placeholder.length(), String.valueOf(params[i]));
         }
      }
      return value.toString();
   }
}
