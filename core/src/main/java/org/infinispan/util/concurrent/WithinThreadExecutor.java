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
package org.infinispan.util.concurrent;

import org.infinispan.util.InfinispanCollections;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An executor that works within the current thread.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @see <a href="http://jcip.net/">Java Concurrency In Practice</a>
 * @since 4.0
 */
public final class WithinThreadExecutor extends AbstractExecutorService {
   private volatile boolean shutDown = false;

   @Override
   public void execute(Runnable command) {
      command.run();
   }

   @Override
   public void shutdown() {
      shutDown = true;
   }

   @Override
   public List<Runnable> shutdownNow() {
      shutDown = true;
      return InfinispanCollections.emptyList();
   }

   @Override
   public boolean isShutdown() {
      return shutDown;
   }

   @Override
   public boolean isTerminated() {
      return shutDown;
   }

   @Override
   public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return shutDown;
   }
}
