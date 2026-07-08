package org.infinispan.server.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.functional.extensions.Person;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.jupiter.InfinispanServerExtension;
import org.infinispan.server.test.jupiter.InfinispanServerExtensionBuilder;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TranscodingIT {

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(1)
               .runMode(ServerRunMode.CONTAINER)
               .mavenArtifacts(Common.JBOSS_MARSHALLING_DEPS)
               .mavenArtifacts(
                     "org.infinispan:infinispan-json-pojo-transcoder:" + Version.getVersion(),
                     "org.infinispan:infinispan-xml-transcoder:" + Version.getVersion(),
                     "com.thoughtworks.xstream:xstream:" + System.getProperty("version.xstream")
               )
               .artifacts(ShrinkWrap.create(JavaArchive.class, "pojo.jar").addClass(Person.class))
               .build();

   private RemoteCache<String, Person> createObjectCache() {
      ConfigurationBuilder cacheBuilder = new ConfigurationBuilder();
      cacheBuilder.encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      cacheBuilder.encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      cacheBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addJavaSerialAllowList(".*");
      return SERVERS.hotrod()
            .withServerConfiguration(cacheBuilder)
            .withClientConfiguration(clientBuilder)
            .withMarshaller(JavaSerializationMarshaller.class)
            .create();
   }

   @Test
   public void testObjectToJson() {
      RemoteCache<String, Person> cache = createObjectCache();
      cache.put("person1", new Person("Mikey", 29));

      RestClient restClient = SERVERS.rest().get();
      RestCacheClient restCache = restClient.cache(cache.getName());

      RestResponse response = restCache.get("person1", MediaType.APPLICATION_JSON_TYPE).toCompletableFuture().join();
      assertEquals(200, response.status(), response.body());
      Json json = Json.read(response.body());
      assertEquals("Mikey", json.at("name").asString());
      assertEquals(29, json.at("age").asInteger());
   }

   @Test
   public void testSerializedObjectToJson() {
      ConfigurationBuilder cacheBuilder = new ConfigurationBuilder();
      cacheBuilder.encoding().key().mediaType(MediaType.APPLICATION_SERIALIZED_OBJECT_TYPE);
      cacheBuilder.encoding().value().mediaType(MediaType.APPLICATION_SERIALIZED_OBJECT_TYPE);
      cacheBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addJavaSerialAllowList(".*");
      RemoteCache<String, Person> cache = SERVERS.hotrod()
            .withServerConfiguration(cacheBuilder)
            .withClientConfiguration(clientBuilder)
            .withMarshaller(JavaSerializationMarshaller.class)
            .create();

      cache.put("person1", new Person("Mouth", 33));

      RestClient restClient = SERVERS.rest().get();
      RestCacheClient restCache = restClient.cache(cache.getName());

      RestResponse response = restCache.get("person1", MediaType.APPLICATION_JSON_TYPE).toCompletableFuture().join();
      assertEquals(200, response.status(), response.body());
      Json json = Json.read(response.body());
      assertEquals("Mouth", json.at("name").asString());
      assertEquals(33, json.at("age").asInteger());
   }

   @Test
   public void testJBossMarshalledObjectToJson() {
      ConfigurationBuilder cacheBuilder = new ConfigurationBuilder();
      cacheBuilder.encoding().key().mediaType(MediaType.APPLICATION_JBOSS_MARSHALLING_TYPE);
      cacheBuilder.encoding().value().mediaType(MediaType.APPLICATION_JBOSS_MARSHALLING_TYPE);
      cacheBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);
      RemoteCache<String, Person> cache = SERVERS.hotrod()
            .withServerConfiguration(cacheBuilder)
            .withMarshaller(GenericJBossMarshaller.class)
            .create();

      cache.put("person1", new Person("Sloth", 30));

      RestClient restClient = SERVERS.rest().get();
      RestCacheClient restCache = restClient.cache(cache.getName());

      RestResponse response = restCache.get("person1", MediaType.APPLICATION_JSON_TYPE).toCompletableFuture().join();
      assertEquals(200, response.status(), response.body());
      Json json = Json.read(response.body());
      assertEquals("Sloth", json.at("name").asString());
      assertEquals(30, json.at("age").asInteger());
   }

   @Test
   public void testObjectToXml() {
      RemoteCache<String, Person> cache = createObjectCache();
      cache.put("person1", new Person("Chunk", 40));

      RestClient restClient = SERVERS.rest().get();
      RestCacheClient restCache = restClient.cache(cache.getName());

      RestResponse response = restCache.get("person1", MediaType.APPLICATION_XML_TYPE).toCompletableFuture().join();
      assertEquals(200, response.status(), response.body());
      String xml = response.body();
      assertTrue(xml.contains("Chunk"));
      assertTrue(xml.contains("40"));
   }

}
