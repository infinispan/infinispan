package org.infinispan.test.integration.remote;

import static org.junit.Assert.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.test.integration.data.Person;
import org.infinispan.test.integration.remote.proto.PersonSchema;
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

   private RemoteCache<String, Person> remoteCache;

   @Before
   public void initialize() {
      remoteCache = createRemoteCache();
   }

   @Test
   public void testPutGetCustomObject() {
      SerializationContext serializationContext = MarshallerUtil.getSerializationContext(remoteCache.getRemoteCacheManager());
      PersonSchema.INSTANCE.registerSchema(serializationContext);
      PersonSchema.INSTANCE.registerMarshallers(serializationContext);

      final Person p = new Person("Martin");
      remoteCache.put("k1", p);
      assertEquals(p.getName(), remoteCache.get("k1").getName());
   }

   private static RemoteCache<String, Person> createRemoteCache() {
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(createConfiguration(), true);
      RemoteCache<String, Person> remoteCache = remoteCacheManager.getCache();
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
