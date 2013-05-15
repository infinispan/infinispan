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

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/*
 * This is slightly different AbstractLoaderConfigurationChildBuilder, as it instantiates a new set of children (async and singletonStore)
 * rather than delegate to existing ones.
 */
public abstract class AbstractStoreConfigurationBuilder<T extends CacheStoreConfiguration, S extends AbstractStoreConfigurationBuilder<T, S>> extends
      AbstractLoaderConfigurationBuilder<T, S> implements CacheStoreConfigurationBuilder<T, S> {

   private static final Log log = LogFactory.getLog(AbstractStoreConfigurationBuilder.class);

   protected final AsyncStoreConfigurationBuilder<S> async;
   protected final SingletonStoreConfigurationBuilder<S> singletonStore;
   protected boolean fetchPersistentState = false;
   protected boolean ignoreModifications = false;
   protected boolean purgeOnStartup = false;
   protected int purgerThreads = 1;
   protected boolean purgeSynchronously = false;

   public AbstractStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
      this.async = new AsyncStoreConfigurationBuilder(this);
      this.singletonStore = new SingletonStoreConfigurationBuilder(this);
   }

   /**
    * Configuration for the async cache loader. If enabled, this provides you with asynchronous
    * writes to the cache store, giving you 'write-behind' caching.
    */
   @Override
   public AsyncStoreConfigurationBuilder<S> async() {
      return async;
   }

   /**
    * SingletonStore is a delegating cache store used for situations when only one instance in a
    * cluster should interact with the underlying store. The coordinator of the cluster will be
    * responsible for the underlying CacheStore. SingletonStore is a simply facade to a real
    * CacheStore implementation. It always delegates reads to the real CacheStore.
    */
   @Override
   public SingletonStoreConfigurationBuilder<S> singletonStore() {
      return singletonStore;
   }

   /**
    * If true, fetch persistent state when joining a cluster. If multiple cache stores are chained,
    * only one of them can have this property enabled. Persistent state transfer with a shared cache
    * store does not make sense, as the same persistent store that provides the data will just end
    * up receiving it. Therefore, if a shared cache store is used, the cache will not allow a
    * persistent state transfer even if a cache store has this property set to true. Finally,
    * setting it to true only makes sense if in a clustered environment, and only 'replication' and
    * 'invalidation' cluster modes are supported.
    */
   @Override
   public S fetchPersistentState(boolean b) {
      this.fetchPersistentState = b;
      return self();
   }

   /**
    * If true, any operation that modifies the cache (put, remove, clear, store...etc) won't be
    * applied to the cache store. This means that the cache store could become out of sync with the
    * cache.
    */
   @Override
   public S ignoreModifications(boolean b) {
      this.ignoreModifications = b;
      return self();
   }

   /**
    * If true, purges this cache store when it starts up.
    */
   @Override
   public S purgeOnStartup(boolean b) {
      this.purgeOnStartup = b;
      return self();
   }

   /**
    * The number of threads to use when purging asynchronously.
    */
   @Override
   public S purgerThreads(int i) {
      this.purgerThreads = i;
      return self();
   }

   /**
    * If true, CacheStore#purgeExpired() call will be done synchronously
    */
   @Override
   public S purgeSynchronously(boolean b) {
      this.purgeSynchronously = b;
      return self();
   }

   @Override
   public void validate() {
      async.validate();
      singletonStore.validate();
      ConfigurationBuilder builder = getBuilder();
      if (!loaders().shared() && !fetchPersistentState && !purgeOnStartup
            && builder.clustering().cacheMode().isClustered())
         log.staleEntriesWithoutFetchPersistentStateOrPurgeOnStartup();

      if (loaders().shared() && !loaders().preload()
            && builder.indexing().enabled()
            && builder.indexing().indexLocalOnly())
         log.localIndexingWithSharedCacheLoaderRequiresPreload();
   }

}
