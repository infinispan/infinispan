package org.infinispan.hotrod;

import org.infinispan.api.Infinispan;
import org.infinispan.hotrod.configuration.HotRodConfiguration;
import org.infinispan.hotrod.impl.HotRodTransport;

/**
 * @since 14.0
 **/
public class HotRod implements Infinispan {
   final HotRodConfiguration configuration;
   final HotRodTransport transport;

   HotRod(HotRodConfiguration configuration) {
      this.configuration = configuration;
      this.transport = new HotRodTransport(configuration);
      this.transport.start();
   }

   @Override
   public HotRodSyncContainer sync() {
      return new HotRodSyncContainer(this);
   }

   @Override
   public HotRodAsyncContainer async() {
      return new HotRodAsyncContainer(this);
   }

   @Override
   public HotRodMutinyContainer mutiny() {
      return new HotRodMutinyContainer(this);
   }

   @Override
   public void close() {
      transport.close();
   }
}
