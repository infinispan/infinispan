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
package org.infinispan.executors;

import org.infinispan.util.TypedProperties;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Creates scheduled executors using the JDK Executors service
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DefaultScheduledExecutorFactory implements ScheduledExecutorFactory {
   final static AtomicInteger counter = new AtomicInteger(0);

   @Override
   public ScheduledExecutorService getScheduledExecutor(Properties p) {
      TypedProperties tp = new TypedProperties(p);
      final String threadNamePrefix = p.getProperty("threadNamePrefix", p.getProperty("componentName", "Thread"));
      final int threadPrio = tp.getIntProperty("threadPriority", Thread.MIN_PRIORITY);

      return Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            Thread th = new Thread(r, "Scheduled-" + threadNamePrefix + "-" + counter.getAndIncrement());
            th.setDaemon(true);
            th.setPriority(threadPrio);
            return th;
         }
      });
   }
}
