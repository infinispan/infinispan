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
