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
package org.infinispan.profiling.testinternals;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Essentially a delegate to an ExecutorService, but a special one that is only used by perf tests so it can be ignored
 * when profiling.
 */
public class TaskRunner {
   ExecutorService exec;

   public TaskRunner(int numThreads) {
      this(numThreads, false);
   }
   public TaskRunner(int numThreads, final boolean warmup) {
      final AtomicInteger counter = new AtomicInteger(0);
      final ThreadGroup tg = new ThreadGroup(Thread.currentThread().getThreadGroup(), warmup ? "WarmupLoadGenerators" : "LoadGenerators");
      this.exec = Executors.newFixedThreadPool(numThreads, new ThreadFactory() {

         public Thread newThread(Runnable r) {
            return new Thread(tg, r, (warmup ? "WarmupLoadGenerator-" : "LoadGenerator-") + counter.incrementAndGet());
         }
      });
   }

   public void execute(Runnable r) {
      exec.execute(r);
   }

   public void stop() throws InterruptedException {
      exec.shutdown();
      while (!exec.awaitTermination(30, TimeUnit.SECONDS)) Thread.sleep(30);
   }
}
