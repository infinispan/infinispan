package org.infinispan.test.integration.as;

import org.infinispan.Version;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.Asset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.spec.se.manifest.ManifestDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Test remote query.
 *
 * @author anistor@redhat.com
 * @since 8.2
 */
@RunWith(Arquillian.class)
public class HotRodQueryIT {

   @Deployment
   public static Archive<?> deployment() {
      return ShrinkWrap
            .create(WebArchive.class, "remote-query.war")
            .addClass(HotRodQueryIT.class)
            .add(manifest(), "META-INF/MANIFEST.MF");
   }

   @Test
   public void testRemoteQuery() throws Exception {
      RemoteCacheManager rcm = createCacheManager();

      SerializationContext serializationContext = ProtoStreamMarshaller.getSerializationContext(rcm);
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
      Query query = qf.from(Person.class)
            .having("name").eq("Adrian").toBuilder()
            .build();
      List<Person> list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(Person.class, list.get(0).getClass());
      assertEquals("Adrian", list.get(0).name);

      rcm.stop();
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies", "org.infinispan.client.hotrod:" + Version.getModuleSlot() + " services, " +
                  "org.infinispan.protostream:" + Version.getModuleSlot() + " services, " +
                  "org.infinispan.query.dsl:" + Version.getModuleSlot() + " services, " +
                  "org.infinispan.commons:" + Version.getModuleSlot() + " services")
            .exportAsString();
      return new StringAsset(manifest);
   }

   private static RemoteCacheManager createCacheManager() {
      return new RemoteCacheManager(createConfiguration(), true);
   }

   private static Configuration createConfiguration() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.addServer().host("127.0.0.1");
      config.marshaller(new ProtoStreamMarshaller());
      return config.build();
   }

   public static class Person {

      @ProtoField(number = 1)
      public String name;

      public Person(String name) {
         this.name = name;
      }

      public Person() {
      }
   }
}
