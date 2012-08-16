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

import java.util.concurrent.TimeUnit;

/**
 * Configuration for the async cache store. If enabled, this provides you with asynchronous writes
 * to the cache store, giving you 'write-behind' caching.
 *
 * @author pmuir
 *
 */
public class AsyncStoreConfigurationBuilder extends AbstractLoaderConfigurationChildBuilder<AsyncStoreConfiguration> {

   private boolean enabled = false;
   private long flushLockTimeout = 1;
   private int modificationQueueSize = 1024;
   private long shutdownTimeout = TimeUnit.SECONDS.toMillis(25);
   private int threadPoolSize = 1;

   AsyncStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * If true, all modifications to this cache store happen asynchronously, on a separate thread.
    */
   public AsyncStoreConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }

   public AsyncStoreConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   public AsyncStoreConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   /**
    * Timeout to acquire the lock which guards the state to be flushed to the cache store
    * periodically. The timeout can be adjusted for a running cache.
    */
   public AsyncStoreConfigurationBuilder flushLockTimeout(long l) {
      this.flushLockTimeout = l;
      return this;
   }

   /**
    * Timeout to acquire the lock which guards the state to be flushed to the cache store
    * periodically. The timeout can be adjusted for a running cache.
    */
   public AsyncStoreConfigurationBuilder flushLockTimeout(long l, TimeUnit unit) {
      return flushLockTimeout(unit.toMillis(l));
   }

   /**
    * Sets the size of the modification queue for the async store. If updates are made at a rate
    * that is faster than the underlying cache store can process this queue, then the async store
    * behaves like a synchronous store for that period, blocking until the queue can accept more
    * elements.
    */
   public AsyncStoreConfigurationBuilder modificationQueueSize(int i) {
      this.modificationQueueSize = i;
      return this;
   }

   /**
    * Timeout to stop the cache store. When the store is stopped it's possible that some
    * modifications still need to be applied; you likely want to set a very large timeout to make
    * sure to not loose data
    */
   public AsyncStoreConfigurationBuilder shutdownTimeout(long l) {
      this.shutdownTimeout = l;
      return this;
   }

   /**
    * Timeout to stop the cache store. When the store is stopped it's possible that some
    * modifications still need to be applied; you likely want to set a very large timeout to make
    * sure to not loose data
    */
   public AsyncStoreConfigurationBuilder shutdownTimeout(long l, TimeUnit unit) {
      return shutdownTimeout(unit.toMillis(l));
   }

   /**
    * Size of the thread pool whose threads are responsible for applying the modifications.
    */
   public AsyncStoreConfigurationBuilder threadPoolSize(int i) {
      this.threadPoolSize = i;
      return this;
   }

   @Override
   public
   void validate() {
   }

   @Override
   public
   AsyncStoreConfiguration create() {
      return new AsyncStoreConfiguration(enabled, flushLockTimeout, modificationQueueSize, shutdownTimeout, threadPoolSize);
   }

   @Override
   public AsyncStoreConfigurationBuilder read(AsyncStoreConfiguration template) {
      this.enabled = template.enabled();
      this.flushLockTimeout = template.flushLockTimeout();
      this.modificationQueueSize = template.modificationQueueSize();
      this.shutdownTimeout = template.shutdownTimeout();
      this.threadPoolSize = template.threadPoolSize();

      return this;
   }

   @Override
   public String toString() {
      return "AsyncLoaderConfigurationBuilder{" +
            "enabled=" + enabled +
            ", flushLockTimeout=" + flushLockTimeout +
            ", modificationQueueSize=" + modificationQueueSize +
            ", shutdownTimeout=" + shutdownTimeout +
            ", threadPoolSize=" + threadPoolSize +
            '}';
   }

}
