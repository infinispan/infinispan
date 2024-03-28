package org.infinispan.embedded;

import org.infinispan.api.Infinispan;
import org.infinispan.api.async.AsyncContainer;
import org.infinispan.api.mutiny.MutinyContainer;
import org.infinispan.api.sync.SyncContainer;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.DefaultCacheManager;

/**
 * @since 15.0
 */
public class Embedded implements Infinispan {
   final DefaultCacheManager cacheManager;

   Embedded(ConfigurationBuilderHolder configuration) {
      this.cacheManager = new DefaultCacheManager(configuration, true);
   }

   Embedded(GlobalConfiguration configuration) {
      ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
      holder.getGlobalConfigurationBuilder().read(configuration);
      this.cacheManager = new DefaultCacheManager(configuration, true);
   }

   @Override
   public SyncContainer sync() {
      return new EmbeddedSyncContainer(this);
   }

   @Override
   public AsyncContainer async() {
      return new EmbeddedAsyncContainer(this);
   }

   @Override
   public MutinyContainer mutiny() {
      return new EmbeddedMutinyContainer(this);
   }

   @Override
   public void close() {
      Util.close(cacheManager);
   }
}
