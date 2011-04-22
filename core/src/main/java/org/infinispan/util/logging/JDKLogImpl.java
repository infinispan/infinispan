/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
