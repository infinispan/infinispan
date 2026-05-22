package org.infinispan.test.integration.thirdparty.remote;

import static org.infinispan.test.integration.thirdparty.DeploymentHelper.addLibrary;
import static org.infinispan.test.integration.thirdparty.DeploymentHelper.createDeployment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteSchemasAdmin;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.test.integration.data.Book;
import org.infinispan.test.integration.data.Person;
import org.infinispan.test.integration.remote.proto.BookQuerySchema;
import org.infinispan.test.integration.remote.proto.PersonSchema;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit5.container.annotation.ArquillianTest;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ContainerFetchException;

/**
 * Tests the Infinispan Hot Rod client integration with third-party application servers.
 * Covers basic cache operations and remote queries (indexed and non-indexed).
 *
 * @since 7.0
 */
@ArquillianTest
public class HotRodIT {

   private static final String HOST = "127.0.0.1";
   private static AutoCloseable SERVER;

   @BeforeAll
   public static void startServer() {
      try {
         InfinispanTestServer server = new InfinispanTestServer();
         server.start();
         SERVER = server;
      } catch (ContainerFetchException e) {
         Assumptions.abort("Container image not available: " + e.getMessage());
      }
   }

   @AfterAll
   public static void stopServer() throws Exception {
      if (SERVER != null) {
         SERVER.close();
      }
   }

   @Deployment
   @TargetsContainer("server-1")
   public static Archive<?> deployment() {
      WebArchive war = createDeployment();
      war.addClass(Person.class);
      war.addClass(Book.class);
      war.addPackage(PersonSchema.class.getPackage().getName());
      war.addPackage(BookQuerySchema.class.getPackage().getName());
      war.addAsResource("org/infinispan/test/person.proto");
      war.addAsResource("org/infinispan/test/book.proto");
      addLibrary(war, "org.infinispan:infinispan-client-hotrod");
      return war;
   }

   @Test
   public void testPutGetCustomObject() {
      ConfigurationBuilder config = serverConfiguration();
      config.marshaller(new ProtoStreamMarshaller());
      try (RemoteCacheManager rcm = new RemoteCacheManager(config.build())) {
         SerializationContext serializationContext = MarshallerUtil.getSerializationContext(rcm);
         PersonSchema.INSTANCE.register(serializationContext);

         RemoteCache<String, Person> cache = rcm.getCache("default");
         cache.clear();
         Person p = new Person("Martin");
         cache.put("k1", p);
         assertEquals(p.getName(), cache.get("k1").getName());
      }
   }

   @Test
   public void testIndexedQuery() {
      BookQuerySchema schema = BookQuerySchema.INSTANCE;
      ConfigurationBuilder config = serverConfiguration();
      config.addContextInitializer(schema);
      try (RemoteCacheManager rcm = new RemoteCacheManager(config.build())) {
         registerSchema(rcm, schema);

         String xmlConfig = """
               <distributed-cache name="books">
                 <indexing>
                   <indexed-entities>
                     <indexed-entity>book_sample.Book</indexed-entity>
                   </indexed-entities>
                 </indexing>
               </distributed-cache>""";
         RemoteCache<Object, Object> cache =
               rcm.administration().getOrCreateCache("books", new StringConfiguration(xmlConfig));

         cache.put(1, new Book("Infinispan in Action", "Learn Infinispan with using it", 2015));
         cache.put(2, new Book("Cloud-Native Applications with Java and Quarkus", "Build robust and reliable cloud applications", 2019));

         Query<Book> query = cache.query("FROM book_sample.Book WHERE title:'java'");
         assertEquals(1, query.list().size());
      }
   }

   @Test
   public void testRemoteQuery() {
      ConfigurationBuilder config = serverConfiguration();
      ProtoStreamMarshaller marshaller = new ProtoStreamMarshaller();
      marshaller.register(PersonSchema.INSTANCE);
      config.marshaller(marshaller);
      try (RemoteCacheManager rcm = new RemoteCacheManager(config.build())) {
         registerSchema(rcm, PersonSchema.INSTANCE);
         RemoteCache<String, Person> cache = rcm.getCache("default");
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

   @Test
   public void testUninverting() {
      ConfigurationBuilder config = serverConfiguration();
      ProtoStreamMarshaller marshaller = new ProtoStreamMarshaller();
      marshaller.register(PersonSchema.INSTANCE);
      config.marshaller(marshaller);
      try (RemoteCacheManager rcm = new RemoteCacheManager(config.build())) {
         registerSchema(rcm, PersonSchema.INSTANCE);
         RemoteCache<String, Person> cache = rcm.getCache("default");
         cache.clear();
         Query<Object> query = cache.query("FROM person_sample.Person WHERE name='John' ORDER BY id");
         assertEquals(0, query.execute().list().size());
      }
   }

   private ConfigurationBuilder serverConfiguration() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.addServer().host(HOST);
      config.security().authentication().username("admin").password("secret".toCharArray());
      return config;
   }

   private void registerSchema(RemoteCacheManager rcm, GeneratedSchema schema) {
      RemoteSchemasAdmin schemas = rcm.administration().schemas();
      schemas.createOrUpdate(schema);
      assertTrue(schemas.retrieveAllSchemaErrors().isEmpty());
   }

}
