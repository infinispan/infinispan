package org.infinispan.server.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class HotRodCacheQueries {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testQueries() throws IOException {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.marshaller(new ProtoStreamMarshaller());
      RemoteCache<String, Person> cache = SERVER_TEST.getHotRodCache(config, CacheMode.DIST_SYNC);
      RemoteCacheManager rcm = cache.getRemoteCacheManager();

      SerializationContext serializationContext = ProtoStreamMarshaller.getSerializationContext(rcm);
      ProtoSchemaBuilder protoSchemaBuilder = new ProtoSchemaBuilder();
      String protoFile = protoSchemaBuilder.fileName("test.proto")
            .addClass(Person.class)
            .build(serializationContext);

      RemoteCache<String, String> metadataCache = rcm.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("test.proto", protoFile);
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      cache.clear();
      cache.put("Adrian", new Person("Adrian", 1));

      assertTrue(cache.containsKey("Adrian"));

      QueryFactory qf = Search.getQueryFactory(cache);
      Query query = qf.from(Person.class)
            .having("name").eq("Adrian")
            .build();
      List<Person> list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(Person.class, list.get(0).getClass());
      assertEquals("Adrian", list.get(0).name);
   }

   public static class Person {

      @ProtoField(number = 1)
      public String name;

      @ProtoField(number = 2)
      public Integer id;

      public Person() {
      }

      public Person(String name, Integer id) {
         this.name = name;
         this.id = id;
      }
   }
}
