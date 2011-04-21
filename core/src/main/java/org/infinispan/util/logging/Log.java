/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
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
 * In addition to the 6 log levels available, this framework also supports parameter interpolation, similar to the JDKs
 * {@link String#format(String, Object...)} method.  What this means is, that the following block:
 * <code> if (log.isTraceEnabled()) { log.trace("This is a message " + message + " and some other value is " + value); }
 * </code>
 * <p/>
 * ... could be replaced with ...
 * <p/>
 * <code> if (log.isTraceEnabled()) log.trace("This is a message %s and some other value is %s", message, value);
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
