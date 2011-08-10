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
package org.infinispan.factories;

import java.util.HashMap;
import java.util.Map;

/**
 * Holder for known named component names.  To be used with {@link org.infinispan.factories.annotations.ComponentName}
 * annotation.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class KnownComponentNames {
   public static final String ASYNC_TRANSPORT_EXECUTOR = "org.infinispan.executors.transport";
   public static final String ASYNC_NOTIFICATION_EXECUTOR = "org.infinispan.executors.notification";
   public static final String EVICTION_SCHEDULED_EXECUTOR = "org.infinispan.executors.eviction";
   public static final String ASYNC_REPLICATION_QUEUE_EXECUTOR = "org.infinispan.executors.replicationQueue";
   public static final String MODULE_COMMAND_INITIALIZERS ="org.infinispan.modules.command.initializers";
   public static final String MODULE_COMMAND_FACTORIES ="org.infinispan.modules.command.factories";
   public static final String GLOBAL_MARSHALLER = "org.infinispan.marshaller.global";
   public static final String CACHE_MARSHALLER = "org.infinispan.marshaller.cache";

   private static final Map<String, Integer> DEFAULT_THREADCOUNTS = new HashMap<String, Integer>(2);
   private static final Map<String, Integer> DEFAULT_THREADPRIO = new HashMap<String, Integer>(4);

   static {
      DEFAULT_THREADCOUNTS.put(ASYNC_NOTIFICATION_EXECUTOR, 1);
      DEFAULT_THREADCOUNTS.put(ASYNC_TRANSPORT_EXECUTOR, 25);

      DEFAULT_THREADPRIO.put(ASYNC_NOTIFICATION_EXECUTOR, Thread.MIN_PRIORITY);
      DEFAULT_THREADPRIO.put(ASYNC_TRANSPORT_EXECUTOR, Thread.NORM_PRIORITY);
      DEFAULT_THREADPRIO.put(EVICTION_SCHEDULED_EXECUTOR, Thread.MIN_PRIORITY);
      DEFAULT_THREADPRIO.put(ASYNC_REPLICATION_QUEUE_EXECUTOR, Thread.NORM_PRIORITY);
   }

   public static int getDefaultThreads(String componentName) {
      return DEFAULT_THREADCOUNTS.get(componentName);
   }

   public static int getDefaultThreadPrio(String componentName) {
      return DEFAULT_THREADPRIO.get(componentName);
   }
}
