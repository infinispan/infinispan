package org.infinispan.server.test.query;

import static org.junit.Assert.assertFalse;

import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.junit.After;
import org.junit.Before;

/**
 * Base class for tests for remote queries over HotRod.
 *
 * @author Adrian Nistor
 * @author Martin Gencur
 */
public abstract class RemoteQueryBaseIT {

   protected final String cacheContainerName;
   protected final String cacheName;

   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Integer, User> remoteCache;
   protected RemoteCacheManagerFactory rcmFactory;

   protected RemoteQueryBaseIT(String cacheContainerName, String cacheName) {
      this.cacheContainerName = cacheContainerName;
      this.cacheName = cacheName;
   }

   protected abstract RemoteInfinispanServer getServer();

   @Before
   public void setUp() throws Exception {
      rcmFactory = new RemoteCacheManagerFactory();
      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer()
            .host(getServer().getHotrodEndpoint().getInetAddress().getHostName())
            .port(getServer().getHotrodEndpoint().getPort())
            .marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = rcmFactory.createManager(clientBuilder);
      remoteCache = remoteCacheManager.getCache(cacheName);

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("sample_bank_account/bank.proto", Util.getResourceAsString("/sample_bank_account/bank.proto", getClass().getClassLoader()));
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));
   }

   @After
   public void tearDown() {
      if (remoteCache != null) {
         remoteCache.clear();
      }
      if (rcmFactory != null) {
         rcmFactory.stopManagers();
      }
      rcmFactory = null;
   }
}
