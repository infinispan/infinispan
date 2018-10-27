package org.infinispan.server.test.client.hotrod;

import static org.junit.Assert.assertEquals;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteCounterManagerFactory;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.SyncStrongCounter;
import org.infinispan.counter.api.SyncWeakCounter;
import org.infinispan.server.test.category.HotRodClustered;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Simple Counter Test
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.4
 */
@RunWith(Arquillian.class)
@Category(HotRodClustered.class)
public class HotRodCounterIT {

   @InfinispanResource("container1")
   RemoteInfinispanServer server1;

   private RemoteCacheManager remoteCacheManager;

   @Before
   public void initialize() {
      if (remoteCacheManager == null) {
         remoteCacheManager = new RemoteCacheManager(createRemoteCacheManagerConfiguration(), true);
      }
   }

   @Test
   public void testCounters() {
      CounterManager counterManager = RemoteCounterManagerFactory.asCounterManager(remoteCacheManager);
      counterManager.defineCounter("c1", CounterConfiguration.builder(CounterType.BOUNDED_STRONG)
            .upperBound(10)
            .initialValue(1)
            .build());

      counterManager.defineCounter("c2", CounterConfiguration.builder(CounterType.WEAK)
            .initialValue(5)
            .build());

      SyncStrongCounter c1 = counterManager.getStrongCounter("c1").sync();
      SyncWeakCounter c2 = counterManager.getWeakCounter("c2").sync();

      assertEquals(1, c1.getValue());
      assertEquals(5, c2.getValue());
   }

   private Configuration createRemoteCacheManagerConfiguration() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.addServer()
            .host(server1.getHotrodEndpoint().getInetAddress().getHostName())
            .port(server1.getHotrodEndpoint().getPort());

      return config.build();
   }

}
