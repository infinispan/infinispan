package org.infinispan.util.logging;

import static org.infinispan.util.Util.formatString;

/**
 * Abstract log implementation that handles message interpolation
 *
 * @author Manik Surtani
 */
public abstract class AbstractLogImpl implements Log {
   public void trace(Object message, Object... params) {
      if (isTraceEnabled()) trace(formatString(message, params));
   }

   public void debug(Object message, Object... params) {
      if (isDebugEnabled()) debug(formatString(message, params));
   }

   public void info(Object message, Object... params) {
      if (isInfoEnabled()) info(formatString(message, params));
   }

   public void warn(Object message, Object... params) {
      if (isWarnEnabled()) warn(formatString(message, params));
   }

   public void error(Object message, Object... params) {
      if (isErrorEnabled()) error(formatString(message, params));
   }

   public void fatal(Object message, Object... params) {
      if (isFatalEnabled()) fatal(formatString(message, params));
   }

   public void trace(Object message, Throwable t, Object... params) {
      if (isTraceEnabled()) trace(formatString(message, params), t);
   }

   public void debug(Object message, Throwable t, Object... params) {
      if (isDebugEnabled()) debug(formatString(message, params), t);
   }

   public void info(Object message, Throwable t, Object... params) {
      if (isInfoEnabled()) info(formatString(message, params), t);
   }

   public void warn(Object message, Throwable t, Object... params) {
      if (isWarnEnabled()) warn(formatString(message, params), t);
   }

   public void error(Object message, Throwable t, Object... params) {
      if (isErrorEnabled()) error(formatString(message, params), t);
   }

   public void fatal(Object message, Throwable t, Object... params) {
      if (isFatalEnabled()) fatal(formatString(message, params), t);
   }
}
