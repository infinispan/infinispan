package org.infinispan.persistence.remote.configuration;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Properties;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;


@Test(groups = "unit", testName = "persistence.remote.configuration.ConfigurationTest")
public class ConfigurationTest {

   public void testRemoteCacheStoreConfigurationAdaptor() {
      Properties p = new Properties();
      p.setProperty("protocolVersion", "2.7");
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.persistence().addStore(RemoteStoreConfigurationBuilder.class)
         .remoteCacheName("RemoteCache")
         .fetchPersistentState(true)
         .addServer()
            .host("one").port(12111)
         .addServer()
            .host("two")
         .connectionPool()
            .maxActive(10)
            .minIdle(5)
            .exhaustedAction(ExhaustedAction.EXCEPTION)
            .minEvictableIdleTime(10_000)
         .async().enable()
         .withProperties(p);

      Configuration configuration = b.build();
      RemoteStoreConfiguration store = (RemoteStoreConfiguration) configuration.persistence().stores().get(0);
      assertEquals("RemoteCache", store.remoteCacheName());
      assertEquals(2, store.servers().size());
      assertEquals(10, store.connectionPool().maxActive());
      assertEquals(5, store.connectionPool().minIdle());
      assertEquals(ExhaustedAction.EXCEPTION, store.connectionPool().exhaustedAction());
      assertEquals(10_000, store.connectionPool().minEvictableIdleTime());
      assertTrue(store.fetchPersistentState());
      assertTrue(store.async().enabled());
      assertEquals(ProtocolVersion.PROTOCOL_VERSION_27, store.protocol());

      b = new ConfigurationBuilder();
      b.persistence().addStore(RemoteStoreConfigurationBuilder.class).read(store);
      Configuration configuration2 = b.build();
      RemoteStoreConfiguration store2 = (RemoteStoreConfiguration) configuration2.persistence().stores().get(0);
      assertEquals("RemoteCache", store2.remoteCacheName());
      assertEquals(2, store2.servers().size());
      assertEquals(10, store2.connectionPool().maxActive());
      assertEquals(5, store2.connectionPool().minIdle());
      assertEquals(ExhaustedAction.EXCEPTION, store2.connectionPool().exhaustedAction());
      assertEquals(10_000, store2.connectionPool().minEvictableIdleTime());
      assertTrue(store2.fetchPersistentState());
      assertTrue(store2.async().enabled());
      assertEquals(ProtocolVersion.PROTOCOL_VERSION_27, store2.protocol());
   }
}
