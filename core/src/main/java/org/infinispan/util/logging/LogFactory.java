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

import org.infinispan.util.Util;
import org.jboss.logging.Logger;
import org.jboss.logging.NDC;

/**
 * Factory that creates {@link Log} instances.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class LogFactory {

   public static final boolean IS_LOG4J_AVAILABLE;

   static {
      boolean available;
      try {
         Util.loadClassStrict("org.apache.log4j.Logger", Thread.currentThread().getContextClassLoader());
         available = true;
      }
      catch (ClassNotFoundException cnfe) {
         available = false;
      }
      IS_LOG4J_AVAILABLE = available;
   }

   public static Log getLog(Class clazz) {
      return Logger.getMessageLogger(Log.class, clazz.getName());
   }

   public static <T> T getLog(Class clazz, Class<T> logClass) {
      return Logger.getMessageLogger(logClass, clazz.getName());
   }

   public static void pushNDC(String cacheName, boolean isTrace) {
      if (isTrace)
         NDC.push(cacheName);
   }

   public static void popNDC(boolean isTrace) {
      if (isTrace)
         NDC.pop();
   }

}
