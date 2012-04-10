/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.client.hotrod.impl.async;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.executors.ExecutorFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default implementation for {@link org.infinispan.executors.ExecutorFactory} based on an {@link ThreadPoolExecutor}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class DefaultAsyncExecutorFactory implements ExecutorFactory {
   public static final String THREAD_NAME = "HotRod-client-async-pool";
   public static final AtomicInteger counter = new AtomicInteger(0);

   @Override
   public ExecutorService getExecutor(Properties p) {
      ConfigurationProperties cp = new ConfigurationProperties(p);
      ThreadFactory tf = new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            Thread th = new Thread(r, THREAD_NAME + "-" + counter.getAndIncrement());
            th.setDaemon(true);
            return th;
         }
      };

      return new ThreadPoolExecutor(cp.getDefaultExecutorFactoryPoolSize(), cp.getDefaultExecutorFactoryPoolSize(),
                                    0L, TimeUnit.MILLISECONDS,
                                    new LinkedBlockingQueue<Runnable>(cp.getDefaultExecutorFactoryQueueSize()),
                                    tf);
   }
}
