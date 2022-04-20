package org.infinispan.test.integration.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.test.integration.data.Book;
import org.infinispan.test.integration.data.Person;
import org.infinispan.test.integration.remote.proto.BookQuerySchema;
import org.infinispan.test.integration.remote.proto.BookQuerySchemaImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 * @since 9.0
 */
public abstract class AbstractHotRodQueryIT {

   private static final String HOST = "127.0.0.1";

   @Test
   public void testIndexed() {
      BookQuerySchema schema = new BookQuerySchemaImpl();
      ConfigurationBuilder config = localServerConfiguration();
      config.addContextInitializer(schema);
      try (RemoteCacheManager remoteCacheManager = new RemoteCacheManager(config.build())) {

         // register schema
         registerSchema(remoteCacheManager, schema.getProtoFileName(), schema.getProtoFile());

         String xmlConfig = "" +
               "<distributed-cache name=\"books\">\n" +
               "  <indexing path=\"${java.io.tmpdir}/index\">\n" +
               "    <indexed-entities>\n" +
               "      <indexed-entity>book_sample.Book</indexed-entity>\n" +
               "    </indexed-entities>\n" +
               "  </indexing>\n" +
               "</distributed-cache>";
         RemoteCache<Object, Object> remoteCache =
               remoteCacheManager.administration().getOrCreateCache("books", new StringConfiguration(xmlConfig));

         // Add some Books
         Book book1 = new Book("Infinispan in Action", "Learn Infinispan with using it", 2015);
         Book book2 = new Book("Cloud-Native Applications with Java and Quarkus", "Build robust and reliable cloud applications", 2019);

         remoteCache.put(1, book1);
         remoteCache.put(2, book2);

         QueryFactory queryFactory = Search.getQueryFactory(remoteCache);
         Query<Book> query = queryFactory.create("FROM book_sample.Book WHERE title:'java'");
         List<Book> list = query.execute().list();
         assertEquals(1, list.size());
      }
   }

   @Test
   public void testRemoteQuery() throws Exception {
      ConfigurationBuilder config = localServerConfiguration();
      config.marshaller(new ProtoStreamMarshaller());
      try (RemoteCacheManager rcm = new RemoteCacheManager(config.build())) {
         // register schema
         SerializationContext serializationContext = MarshallerUtil.getSerializationContext(rcm);
         String protoKey = "test.proto";
         ProtoSchemaBuilder protoSchemaBuilder = new ProtoSchemaBuilder();
         String protoFile = protoSchemaBuilder.fileName(protoKey)
               .addClass(Person.class)
               .build(serializationContext);
         registerSchema(rcm, protoKey, protoFile);

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
   }

   /**
    * Sorting on a field that does not contain DocValues so Hibernate Search is forced to uninvert it.
    *
    * @see <a href="https://issues.jboss.org/browse/ISPN-5729">https://issues.jboss.org/browse/ISPN-5729</a>
    */
   @Test
   public void testUninverting() throws Exception {
      ConfigurationBuilder config = localServerConfiguration();
      config.marshaller(new ProtoStreamMarshaller());
      try (RemoteCacheManager rcm = new RemoteCacheManager(config.build())) {
         // register schema
         SerializationContext serializationContext = MarshallerUtil.getSerializationContext(rcm);
         String protoKey = "test.proto";
         ProtoSchemaBuilder protoSchemaBuilder = new ProtoSchemaBuilder();
         String protoFile = protoSchemaBuilder.fileName(protoKey)
               .addClass(Person.class)
               .build(serializationContext);
         registerSchema(rcm, protoKey, protoFile);

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

   private ConfigurationBuilder localServerConfiguration() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.addServer().host(HOST);
      return config;
   }

   private void registerSchema(RemoteCacheManager rcm, String key, String protoFile) {
      RemoteCache<String, String> metadataCache = rcm.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put(key, protoFile);
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
   }
}
