package org.infinispan.server.test.client.hotrod;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.multimap.MultimapCacheManager;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCache;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCacheManagerFactory;
import org.infinispan.server.test.category.HotRodClustered;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Simple Multimap Test
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.4
 */
@RunWith(Arquillian.class)
@Category(HotRodClustered.class)
public class HotRodMultimapIT {

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
   public void testMultimap() throws Exception {
      remoteCacheManager.administration().getOrCreateCache("cutes", "default");
      MultimapCacheManager multimapCacheManager = RemoteMultimapCacheManagerFactory.from(remoteCacheManager);

      RemoteMultimapCache<Integer, String> people = multimapCacheManager.get("cutes");
      people.put(1, "Elaia");
      people.put(1, "Oihana");

      Collection<String> littles = people.get(1).join();

      assertEquals(2, littles.size());
      assertTrue(littles.contains("Elaia"));
      assertTrue(littles.contains("Oihana"));
   }

   private Configuration createRemoteCacheManagerConfiguration() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.addServer()
            .host(server1.getHotrodEndpoint().getInetAddress().getHostName())
            .port(server1.getHotrodEndpoint().getPort());

      return config.build();
   }

}
