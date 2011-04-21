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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Logger that delivers messages to a Log4J logger
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class Log4JLogImpl extends AbstractLogImpl {

   private final Logger logger;

   private static final Level TRACE;

   static {
      Object trace;
      try {
         trace = Level.class.getDeclaredField("TRACE").get(null);
      }
      catch (Exception e) {
         trace = Level.DEBUG;
      }
      TRACE = (Level) trace;
   }

   public Log4JLogImpl(String category) {
      logger = Logger.getLogger(category);
   }

   public void trace(Object message) {
      logger.log(Log4JLogImpl.class.getName(), TRACE, message, null);
   }

   public void debug(Object message) {
      logger.log(Log4JLogImpl.class.getName(), Level.DEBUG, message, null);
   }

   public void info(Object message) {
      logger.log(Log4JLogImpl.class.getName(), Level.INFO, message, null);
   }

   public void warn(Object message) {
      logger.log(Log4JLogImpl.class.getName(), Level.WARN, message, null);
   }

   public void error(Object message) {
      logger.log(Log4JLogImpl.class.getName(), Level.ERROR, message, null);
   }

   public void fatal(Object message) {
      logger.log(Log4JLogImpl.class.getName(), Level.FATAL, message, null);
   }

   public void trace(Object message, Throwable t) {
      logger.log(Log4JLogImpl.class.getName(), TRACE, message, t);
   }

   public void debug(Object message, Throwable t) {
      logger.log(Log4JLogImpl.class.getName(), Level.DEBUG, message, t);
   }

   public void info(Object message, Throwable t) {
      logger.log(Log4JLogImpl.class.getName(), Level.INFO, message, t);
   }

   public void warn(Object message, Throwable t) {
      logger.log(Log4JLogImpl.class.getName(), Level.WARN, message, t);
   }

   public void error(Object message, Throwable t) {
      logger.log(Log4JLogImpl.class.getName(), Level.ERROR, message, t);
   }

   public void fatal(Object message, Throwable t) {
      logger.log(Log4JLogImpl.class.getName(), Level.FATAL, message, t);
   }

   public boolean isTraceEnabled() {
      return logger.isEnabledFor(TRACE);
   }

   public boolean isDebugEnabled() {
      return logger.isEnabledFor(Level.DEBUG);
   }

   public boolean isInfoEnabled() {
      return logger.isEnabledFor(Level.INFO);
   }

   public boolean isWarnEnabled() {
      return logger.isEnabledFor(Level.WARN);
   }

   public boolean isErrorEnabled() {
      return logger.isEnabledFor(Level.ERROR);
   }

   public boolean isFatalEnabled() {
      return logger.isEnabledFor(Level.FATAL);
   }
}
