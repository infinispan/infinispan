package org.infinispan.server.test.client.hotrod.osgi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.maven;

import java.io.File;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.infinispan.Version;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
import org.infinispan.protostream.sampledomain.Address;
import org.infinispan.protostream.sampledomain.Note;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.protostream.sampledomain.marshallers.AccountMarshaller;
import org.infinispan.protostream.sampledomain.marshallers.AddressMarshaller;
import org.infinispan.protostream.sampledomain.marshallers.GenderMarshaller;
import org.infinispan.protostream.sampledomain.marshallers.LimitsMarshaller;
import org.infinispan.protostream.sampledomain.marshallers.TransactionMarshaller;
import org.infinispan.protostream.sampledomain.marshallers.UserMarshaller;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.test.category.Osgi;
import org.infinispan.server.test.util.osgi.KarafTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.ProbeBuilder;
import org.ops4j.pax.exam.TestProbeBuilder;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.karaf.options.KarafDistributionOption;
import org.ops4j.pax.exam.options.RawUrlReference;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;

/**
 * Simple test for RemoteCache running in OSGi (Karaf). Both basic put/get operations and remote
 * queries are tested.
 *
 * @author mgencur
 * @author anistor@redhat.com
 */
@RunWith(PaxExam.class)
@Category(Osgi.class)
@ExamReactorStrategy(PerClass.class)
public class RemoteCacheOsgiIT extends KarafTestSupport {

   private final String SERVER_HOST = "localhost";
   private final int HOTROD_PORT = 11222;
   private final String DEFAULT_CACHE = "localnotindexed";
   private final String INDEXED_CACHE = "localtestcache";
   private final String KARAF_VERSION = System.getProperty("version.karaf", "2.3.3");
   private final String PROTOSTREAM_VERSION = System.getProperty("version.protostream", "4.2.2.Final");
   private final String RESOURCES_DIR = System.getProperty("resources.dir", System.getProperty("java.io.tmpdir"));
   private ConfigurationBuilder builder;
   private RemoteCacheManager manager;

   @Configuration
   public Option[] config() throws Exception {
      return new Option[] {
            KarafDistributionOption
                  .karafDistributionConfiguration()
                  .unpackDirectory(new File("target/pax"))
                  .frameworkUrl(
                        maven().groupId("org.apache.karaf").artifactId("apache-karaf").type("tar.gz")
                              .version(KARAF_VERSION)).karafVersion(KARAF_VERSION),
            KarafDistributionOption.features(maven().groupId("org.infinispan").artifactId("infinispan-remote").version(Version.getVersion())
                                                   .type("xml").classifier("features"), "infinispan-remote"),
            mavenBundle("org.infinispan.protostream", "sample-domain-implementation", PROTOSTREAM_VERSION),
            mavenBundle("org.infinispan.protostream", "sample-domain-definition", PROTOSTREAM_VERSION),

            KarafDistributionOption.features(new RawUrlReference("file:///" + RESOURCES_DIR.replace("\\", "/")
                  + "/test-features.xml"), "query-sample-domain"),
            KarafDistributionOption.editConfigurationFileExtend("etc/jre.properties", "jre-1.7", "sun.misc"),
            KarafDistributionOption.editConfigurationFileExtend("etc/jre.properties", "jre-1.6", "sun.misc"),
            KarafDistributionOption.keepRuntimeFolder(),
            localRepoForPAXUrl()
      };
   }

   @ProbeBuilder
   public static TestProbeBuilder exportTestPackages(TestProbeBuilder probeBuilder) {
      probeBuilder.setHeader("Export-Package", RemoteCacheOsgiIT.class.getPackage().getName());
      return probeBuilder;
   }

   @Before
   public void setUp() {
      builder = new ConfigurationBuilder();
      builder.addServer().host(SERVER_HOST).port(HOTROD_PORT);
   }

   @After
   public void tearDown() {
      if (manager != null) {
         manager.stop();
      }
   }

   @Test
   public void testCustomObjectPutGet() {
      Person p = new Person("Martin");
      manager = new RemoteCacheManager(builder.build());
      RemoteCache<Object, Object> cache = manager.getCache(DEFAULT_CACHE);
      cache.put("k1", p);
      assertEquals(p, cache.get("k1"));
   }

   @Test
   public void testAttributeQuery() throws Exception {
      builder.marshaller(new ProtoStreamMarshaller());
      manager = new RemoteCacheManager(builder.build());
      RemoteCache<Integer, Object> cache = manager.getCache(INDEXED_CACHE);

      // register schemas and marshallers on client
      String bankSchemaFile = Util.read(bundleContext.getBundle().getResource("/sample_bank_account/bank.proto").openStream());
      FileDescriptorSource fds = new FileDescriptorSource();
      fds.addProtoFile("sample_bank_account/bank.proto", bankSchemaFile);
      SerializationContext ctx = ProtoStreamMarshaller.getSerializationContext(manager);
      ctx.registerProtoFiles(fds);
      ctx.registerMarshaller(new UserMarshaller());
      ctx.registerMarshaller(new GenderMarshaller());
      ctx.registerMarshaller(new AddressMarshaller());
      ctx.registerMarshaller(new AccountMarshaller());
      ctx.registerMarshaller(new LimitsMarshaller());
      ctx.registerMarshaller(new TransactionMarshaller());

      ProtoSchemaBuilder protoSchemaBuilder = new ProtoSchemaBuilder();
      String testSchemaFile = protoSchemaBuilder.fileName("test.proto")
            .addClass(Note.class)
            .build(ctx);

      // register schemas on server
      RemoteCache<String, String> metadataCache = manager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("sample_bank_account/bank.proto", bankSchemaFile);
      metadataCache.put("test.proto", testSchemaFile);
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      cache.put(1, createUser1());
      cache.put(2, createUser2());
      cache.put(3, createNote());

      // get User back from remote cache and check its attributes
      User userFromCache = (User) cache.get(1);
      assertUser(userFromCache);

      // get Note back from remote cache and check its attributes
      Note noteFromCache = (Note) cache.get(3);
      assertNote(noteFromCache);

      // get User back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(cache);
      Query query = qf.from(User.class).having("name").eq("Tom").build();
      List list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(User.class, list.get(0).getClass());
      assertUser((User) list.get(0));

      // get Note back from remote cache via query and check its attributes
      query = qf.from(Note.class).having("author.name").eq("name").build();
      list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(Note.class, list.get(0).getClass());
      assertNote((Note) list.get(0));
   }

   private Note createNote() {
      Note note = new Note();
      note.setText("testing 123");
      User author = new User();
      author.setId(20);
      author.setName("name");
      author.setSurname("surname");
      note.setAuthor(author);
      return note;
   }

   private void assertNote(Note note) {
      assertNotNull(note);
      assertEquals("testing 123", note.getText());
      assertNotNull(note.getAuthor());
      assertEquals(20, note.getAuthor().getId());
      assertEquals("name", note.getAuthor().getName());
      assertEquals("surname", note.getAuthor().getSurname());
   }

   private User createUser1() {
      User user = new User();
      user.setId(1);
      user.setName("Tom");
      user.setSurname("Cat");
      user.setGender(User.Gender.MALE);
      user.setAccountIds(Collections.singleton(12));
      Address address = new Address();
      address.setStreet("Dark Alley");
      address.setPostCode("1234");
      user.setAddresses(Collections.singletonList(address));
      return user;
   }

   private User createUser2() {
      User user = new User();
      user.setId(2);
      user.setName("Adrian");
      user.setSurname("Nistor");
      user.setGender(User.Gender.MALE);
      Address address = new Address();
      address.setStreet("Old Street");
      address.setPostCode("XYZ");
      user.setAddresses(Collections.singletonList(address));
      return user;
   }

   private void assertUser(User user) {
      assertNotNull(user);
      assertEquals(1, user.getId());
      assertEquals("Tom", user.getName());
      assertEquals("Cat", user.getSurname());
      assertEquals(User.Gender.MALE, user.getGender());
      assertNotNull(user.getAccountIds());
      assertEquals(1, user.getAccountIds().size());
      assertTrue(user.getAccountIds().contains(12));
      assertNotNull(user.getAddresses());
      assertEquals(1, user.getAddresses().size());
      assertEquals("Dark Alley", user.getAddresses().get(0).getStreet());
      assertEquals("1234", user.getAddresses().get(0).getPostCode());
   }

   static class Person implements Serializable {

      final String name;

      public Person(String name) {
         this.name = name;
      }

      public String getName() {
         return name;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o)
            return true;
         if (o == null || getClass() != o.getClass())
            return false;

         Person person = (Person) o;

         return name.equals(person.name);
      }

      @Override
      public int hashCode() {
         return name.hashCode();
      }
   }

}
