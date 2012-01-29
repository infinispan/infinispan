/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.configuration.cache;

/**
 * Configuration for the async cache loader. If enabled, this provides you with asynchronous writes
 * to the cache store, giving you 'write-behind' caching.
 * 
 * @author pmuir
 * 
 */
public class AsyncLoaderConfiguration {

   private final boolean enabled;
   private long flushLockTimeout;
   private final int modificationQueueSize;
   private long shutdownTimeout;
   private final int threadPoolSize;

   AsyncLoaderConfiguration(boolean enabled, long flushLockTimeout, int modificationQueueSize, long shutdownTimeout,
         int threadPoolSize) {
      this.enabled = enabled;
      this.flushLockTimeout = flushLockTimeout;
      this.modificationQueueSize = modificationQueueSize;
      this.shutdownTimeout = shutdownTimeout;
      this.threadPoolSize = threadPoolSize;
   }

   /**
    * If true, all modifications to this cache store happen asynchronously, on a separate thread.
    */
   public boolean enabled() {
      return enabled;
   }

   /**
    * Timeout to acquire the lock which guards the state to be flushed to the cache store
    * periodically. The timeout can be adjusted for a running cache.
    * 
    * @return
    */
   public long flushLockTimeout() {
      return flushLockTimeout;
   }

   /**
    * Timeout to acquire the lock which guards the state to be flushed to the cache store
    * periodically. The timeout can be adjusted for a running cache.
    */
   public AsyncLoaderConfiguration flushLockTimeout(long l) {
      this.flushLockTimeout = l;
      return this;
   }

   /**
    * Sets the size of the modification queue for the async store. If updates are made at a rate
    * that is faster than the underlying cache store can process this queue, then the async store
    * behaves like a synchronous store for that period, blocking until the queue can accept more
    * elements.
    */
   public int modificationQueueSize() {
      return modificationQueueSize;
   }

   /**
    * Timeout to stop the cache store. When the store is stopped it's possible that some
    * modifications still need to be applied; you likely want to set a very large timeout to make
    * sure to not loose data
    */
   public long shutdownTimeout() {
      return shutdownTimeout;
   }

   public AsyncLoaderConfiguration shutdownTimeout(long l) {
      this.shutdownTimeout = l;
      return this;
   }

   /**
    * Size of the thread pool whose threads are responsible for applying the modifications.
    */
   public int threadPoolSize() {
      return threadPoolSize;
   }

}
