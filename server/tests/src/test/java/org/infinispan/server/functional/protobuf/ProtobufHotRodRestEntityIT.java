package org.infinispan.server.functional.protobuf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.infinispan.server.test.core.Common.sync;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public class ProtobufHotRodRestEntityIT {

   private static final String CACHE_NAME = "test";

   private static final String CACHE_CONFIG =
         "<distributed-cache name=\"CACHE_NAME\">\n"
               + "    <encoding media-type=\"application/x-protostream\"/>\n"
               + "</distributed-cache>";

   @ClassRule
   public static InfinispanServerRule SERVERS = InfinispanServerRuleBuilder.config("configuration/BasicServerTest.xml")
         .runMode(ServerRunMode.FORKED)
         .numServers(1)
         .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void test() {
      RemoteCacheManager hotRodClient = getHotRodClient();
      try {
         testQueries(hotRodClient);
      } finally {
         hotRodClient.stop();
      }

      RestClient restClient = SERVER_TEST.rest().create();
      RestResponse response = sync(restClient.cache(CACHE_NAME).entries(1000));

      if (response.getStatus() != 200) {
         fail(response.getBody());
      }

      Collection<?> entities = (Collection<?>) Json.read(response.getBody()).getValue();
      assertThat(entities).hasSize(4);
   }

   private static void testQueries(RemoteCacheManager client) {
      RemoteCache<String, Person> peopleCache = client.getCache(CACHE_NAME);

      Map<String, Person> people = new HashMap<>();
      people.put("1", new Person("Oihana", "Rossignol", 2016, "Paris"));
      people.put("2", new Person("Elaia", "Rossignol", 2018, "Paris"));
      people.put("3", new Person("Yago", "Steiner", 2013, "Saint-Mandé"));
      people.put("4", new Person("Alberto", "Steiner", 2016, "Paris"));
      peopleCache.putAll(people);

      QueryFactory queryFactory = Search.getQueryFactory(peopleCache);
      Query<Person> query = queryFactory.create("FROM tutorial.Person p where p.lastName = :lastName");
      query.setParameter("lastName", "Rossignol");
      List<Person> rossignols = query.execute().list();
      assertThat(rossignols).extracting("firstName").containsExactlyInAnyOrder("Oihana", "Elaia");
   }

   private static RemoteCacheManager getHotRodClient() {
      GeneratedSchema schema = new QuerySchemaBuilderImpl();

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.remoteCache(CACHE_NAME).configuration(CACHE_CONFIG.replace("CACHE_NAME", CACHE_NAME));

      builder.addContextInitializer(schema);
      RemoteCacheManager client = new RemoteCacheManager(builder.build());

      RemoteCache<String, String> metadataCache = client.getCache(PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put(schema.getProtoFileName(), schema.getProtoFile());
      return client;
   }
}
