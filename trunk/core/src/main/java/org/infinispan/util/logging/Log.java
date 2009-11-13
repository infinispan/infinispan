package org.infinispan.util.logging;

/**
 * Infinispan's log abstraction layer.
 * <p/>
 * Usage is very similar to Apache's Commons Logging, except that there are no additional dependencies beyond a JDK.
 * <p/>
 * <code> Log log = LogFactory.getLog( getClass() ); </code> The above will get you an instance of <tt>Log</tt>, which
 * can be used to generate log messages either to Log4J (if the libraries are present) or (if not) the built-in JDK
 * logger.
 * <p/>
 * In addition to the 6 log levels available, this framework also supports parameter interpolation, inspired by <a
 * href="http://www.seamframework.org">SEAM</a>'s similar approach.  What this means is, that the following block:
 * <code> if (log.isTraceEnabled()) { log.trace("This is a message " + message + " and some other value is " + value); }
 * </code>
 * <p/>
 * ... could be replaced with ...
 * <p/>
 * <code> if (log.isTraceEnabled()) log.trace("This is a message {0} and some other value is {1}", message, value);
 * </code>
 * <p/>
 * This greatly enhances code readability.
 * <p/>
 * If you are passing a <tt>Throwable</tt>, note that this should be passed in <i>before</i> the vararg parameter list.
 * <p/>
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface Log {

   // methods that support parameter substitution

   void trace(Object message, Object... params);

   void debug(Object message, Object... params);

   void info(Object message, Object... params);

   void warn(Object message, Object... params);

   void error(Object message, Object... params);

   void fatal(Object message, Object... params);

   void trace(Object message, Throwable t, Object... params);

   void debug(Object message, Throwable t, Object... params);

   void info(Object message, Throwable t, Object... params);

   void warn(Object message, Throwable t, Object... params);

   void error(Object message, Throwable t, Object... params);

   void fatal(Object message, Throwable t, Object... params);

   // methods that do not support parameter substitution

   void trace(Object message);

   void debug(Object message);

   void info(Object message);

   void warn(Object message);

   void error(Object message);

   void fatal(Object message);

   void trace(Object message, Throwable t);

   void debug(Object message, Throwable t);

   void info(Object message, Throwable t);

   void warn(Object message, Throwable t);

   void error(Object message, Throwable t);

   void fatal(Object message, Throwable t);

   // methods to test log levels

   boolean isTraceEnabled();

   boolean isDebugEnabled();

   boolean isInfoEnabled();

   boolean isWarnEnabled();

   boolean isErrorEnabled();

   boolean isFatalEnabled();
}
