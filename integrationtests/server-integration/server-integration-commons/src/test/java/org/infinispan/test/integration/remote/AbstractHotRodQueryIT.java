package org.infinispan.test.integration.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.test.integration.data.Book;
import org.infinispan.test.integration.data.Person;
import org.infinispan.test.integration.remote.proto.BookQuerySchema;
import org.infinispan.test.integration.remote.proto.PersonSchema;
import org.junit.Assert;
import org.junit.Test;

/**
 * @since 9.0
 */
public abstract class AbstractHotRodQueryIT {

   private static final String HOST = "127.0.0.1";

   @Test
   public void testIndexed() {
      BookQuerySchema schema = BookQuerySchema.INSTANCE;
      ConfigurationBuilder config = localServerConfiguration();
      config.addContextInitializer(schema);
      try (RemoteCacheManager remoteCacheManager = new RemoteCacheManager(config.build())) {

         // register schema
         registerSchema(remoteCacheManager, schema.getProtoFileName(), schema.getProtoFile());

         String xmlConfig = """
               <distributed-cache name="books">
                 <indexing>
                   <indexed-entities>
                     <indexed-entity>book_sample.Book</indexed-entity>
                   </indexed-entities>
                 </indexing>
               </distributed-cache>""";
         RemoteCache<Object, Object> remoteCache =
               remoteCacheManager.administration().getOrCreateCache("books", new StringConfiguration(xmlConfig));

         // Add some Books
         Book book1 = new Book("Infinispan in Action", "Learn Infinispan with using it", 2015);
         Book book2 = new Book("Cloud-Native Applications with Java and Quarkus", "Build robust and reliable cloud applications", 2019);

         remoteCache.put(1, book1);
         remoteCache.put(2, book2);

         Query<Book> query = remoteCache.query("FROM book_sample.Book WHERE title:'java'");
         List<Book> list = query.list();
         assertEquals(1, list.size());
      }
   }

   @Test
   public void testRemoteQuery() {
      ConfigurationBuilder config = localServerConfiguration();
      config.marshaller(new ProtoStreamMarshaller());
      try (RemoteCacheManager rcm = new RemoteCacheManager(config.build())) {
         // register schema
         SerializationContext serializationContext = MarshallerUtil.getSerializationContext(rcm);
         PersonSchema.INSTANCE.registerMarshallers(serializationContext);
         PersonSchema.INSTANCE.registerSchema(serializationContext);
         registerSchema(rcm, PersonSchema.INSTANCE.getProtoFileName(), PersonSchema.INSTANCE.getProtoFile());

         RemoteCache<String, Person> cache = rcm.getCache();
         cache.clear();
         cache.put("Adrian", new Person("Adrian"));

         assertTrue(cache.containsKey("Adrian"));
         Query<Person> query = cache.query("FROM person_sample.Person WHERE name='Adrian'");
         List<Person> list = query.list();
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
   public void testUninverting() {
      ConfigurationBuilder config = localServerConfiguration();
      config.marshaller(new ProtoStreamMarshaller());
      try (RemoteCacheManager rcm = new RemoteCacheManager(config.build())) {
         SerializationContext serializationContext = MarshallerUtil.getSerializationContext(rcm);
         PersonSchema.INSTANCE.registerMarshallers(serializationContext);
         PersonSchema.INSTANCE.registerSchema(serializationContext);
         registerSchema(rcm, PersonSchema.INSTANCE.getProtoFileName(), PersonSchema.INSTANCE.getProtoFile());

         RemoteCache<String, Person> cache = rcm.getCache();
         cache.clear();
         Query<Object> query = cache.query("FROM FROM person_sample.Person WHERE name='John' ORDER BY id");
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
