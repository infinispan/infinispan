package org.infinispan.test.integration.remote;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
import org.infinispan.test.integration.data.Person;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the Infinispan AS remote client module integration
 *
 * @author Pedro Ruivo
 * @author Richard Achmatowicz
 * @author Martin Gencur
 * @author Jozef Vilkolak
 * @since 7.0
 */
public abstract class AbstractHotRodClientIT {

   private RemoteCache remoteCache;

   @Before
   public void initialize() {
      remoteCache = createRemoteCache();
   }

   @Test
   public void testPutGetCustomObject() throws IOException {
      SerializationContext serializationContext = MarshallerUtil.getSerializationContext(remoteCache.getRemoteCacheManager());
      ProtoSchemaBuilder protoSchemaBuilder = new ProtoSchemaBuilder();
      String protoFile = protoSchemaBuilder.fileName("test.proto")
            .addClass(Person.class)
            .build(serializationContext);
      remoteCache.put("test.proto", protoFile);

      final Person p = new Person("Martin");
      remoteCache.put("k1", p);
      assertEquals(p.getName(), ((Person) remoteCache.get("k1")).getName());
   }

   private static RemoteCache createRemoteCache() {
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(createConfiguration(), true);
      RemoteCache remoteCache = remoteCacheManager.getCache();
      remoteCache.clear();
      return remoteCache;
   }

   private static Configuration createConfiguration() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.addServer().host("127.0.0.1");
      config.marshaller(new ProtoStreamMarshaller());
      return config.build();
   }
}
