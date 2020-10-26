package org.infinispan.test.integration.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.test.integration.data.Person;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * @since 9.0
 */
public abstract class AbstractHotRodQueryIT {

   private RemoteCacheManager rcm;

   private static RemoteCacheManager createCacheManager() {
      return new RemoteCacheManager(createConfiguration(), true);
   }

   private static Configuration createConfiguration() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.addServer().host("127.0.0.1");
      config.marshaller(new ProtoStreamMarshaller());
      return config.build();
   }

   @After
   public void cleanUp() {
      if (rcm != null)
         rcm.stop();
   }

   @Test
   public void testRemoteQuery() throws Exception {
      rcm = createCacheManager();

      SerializationContext serializationContext = MarshallerUtil.getSerializationContext(rcm);
      ProtoSchemaBuilder protoSchemaBuilder = new ProtoSchemaBuilder();
      String protoFile = protoSchemaBuilder.fileName("test.proto")
            .addClass(Person.class)
            .build(serializationContext);

      RemoteCache<String, String> metadataCache = rcm.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("test.proto", protoFile);
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      RemoteCache<String, Person> cache = rcm.getCache();
      cache.clear();
      cache.put("Adrian", new Person("Adrian"));

      assertTrue(cache.containsKey("Adrian"));

      QueryFactory qf = Search.getQueryFactory(cache);
      Query<Person> query = qf.from(Person.class)
            .having("name").eq("Adrian")
            .build();
      List<Person> list = query.execute().list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(Person.class, list.get(0).getClass());
      assertEquals("Adrian", list.get(0).name);
   }

   /**
    * Sorting on a field that does not contain DocValues so Hibernate Search is forced to uninvert it.
    *
    * @see <a href="https://issues.jboss.org/browse/ISPN-5729">https://issues.jboss.org/browse/ISPN-5729</a>
    */
   @Test
   public void testUninverting() throws Exception {
      rcm = createCacheManager();

      SerializationContext serializationContext = MarshallerUtil.getSerializationContext(rcm);
      ProtoSchemaBuilder protoSchemaBuilder = new ProtoSchemaBuilder();
      String protoFile = protoSchemaBuilder.fileName("test.proto")
            .addClass(Person.class)
            .build(serializationContext);

      RemoteCache<String, String> metadataCache = rcm.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("test.proto", protoFile);
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      RemoteCache<String, Person> cache = rcm.getCache();
      cache.clear();

      QueryFactory qf = Search.getQueryFactory(cache);
      Query<Person> query = qf.from(Person.class)
            .having("name").eq("John")
            .orderBy("id")
            .build();
      Assert.assertEquals(0, query.execute().list().size());
   }
}
