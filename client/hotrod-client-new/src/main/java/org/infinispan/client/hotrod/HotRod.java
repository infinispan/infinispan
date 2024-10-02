package org.infinispan.client.hotrod;

import org.infinispan.api.Experimental;
import org.infinispan.api.Infinispan;
import org.infinispan.api.async.AsyncContainer;
import org.infinispan.api.mutiny.MutinyContainer;
import org.infinispan.api.sync.SyncContainer;
import org.infinispan.client.hotrod.configuration.Configuration;

/**
 * @since 14.0
 **/
@Experimental
final class HotRod implements Infinispan {
   final Configuration configuration;
   final RemoteCacheManager cacheManager;

   HotRod(Configuration configuration) {
      this(configuration, new RemoteCacheManager(configuration));
   }

   HotRod(Configuration configuration, RemoteCacheManager rcm) {
      this.configuration = configuration;
      this.cacheManager = rcm;
      this.cacheManager.start();
   }

   @Override
   public SyncContainer sync() {
      return new HotRodSyncContainer(this);
   }

   @Override
   public AsyncContainer async() {
      return new HotRodAsyncContainer(this);
   }

   @Override
   public MutinyContainer mutiny() {
      return new HotRodMutinyContainer(this);
   }

   @Override
   public void close() {
      cacheManager.close();
   }
}
