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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default executor factory that creates executors using the JDK Executors service. However, it is possible to
 * configure the minThread and the keepAliveTime
 * (not possible in {@link org.infinispan.distexec.DefaultExecutorService})
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DefaultDynamicExecutorFactory implements ExecutorFactory {
   private final static AtomicInteger counter = new AtomicInteger(0);

   public ExecutorService getExecutor(Properties p) {
      TypedProperties tp = TypedProperties.toTypedProperties(p);
      int minThreads = tp.getIntProperty("minThreads", 1);
      int maxThreads = tp.getIntProperty("maxThreads", 10);
      int queueSize = tp.getIntProperty("queueSize", 100000);
      long keepAliveTime = tp.getIntProperty("keepAliveTime", 100000);
      final int threadPrio = tp.getIntProperty("threadPriority", Thread.NORM_PRIORITY);

      final String threadNamePrefix = tp.getProperty("threadNamePrefix", tp.getProperty("componentName", "Thread"));

      ThreadFactory tf = new ThreadFactory() {
         public Thread newThread(Runnable r) {
            Thread th = new Thread(r, threadNamePrefix + "-" + counter.getAndIncrement());
            th.setDaemon(true);
            th.setPriority(threadPrio);
            return th;
         }
      };

      return new ThreadPoolExecutor(minThreads, maxThreads,
                                    keepAliveTime, TimeUnit.MILLISECONDS,
                                    new LinkedBlockingQueue<Runnable>(queueSize),
                                    tf);
   }
}
