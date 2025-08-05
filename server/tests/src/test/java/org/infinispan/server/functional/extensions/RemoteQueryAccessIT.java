package org.infinispan.server.functional.extensions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.test.core.Common.createQueryableCache;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.sampledomain.Address;
import org.infinispan.protostream.sampledomain.TestDomainSCIImpl;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.server.functional.hotrod.HotRodCacheQueries;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.tasks.ServerTask;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class RemoteQueryAccessIT {

   public static final String ENTITY_USER = HotRodCacheQueries.ENTITY_USER;

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(3)
               .runMode(ServerRunMode.CONTAINER)
               .artifacts(artifacts())
               .build();

   public static JavaArchive[] artifacts() {
      JavaArchive statistics = ShrinkWrap.create(JavaArchive.class, "remote-query-access-with-stats.jar")
            .addClass(RemoteQueryAccessWithStatsTask.class)
            .addPackage("org.infinispan.protostream.sampledomain")
            .addAsServiceProvider(ServerTask.class, RemoteQueryAccessWithStatsTask.class)
            .addAsServiceProvider(SerializationContextInitializer.class, TestDomainSCIImpl.class)
            .addAsResource("org/infinispan/test/test.protostream.sampledomain.proto");

      return new JavaArchive[]{statistics};
   }

   @Test
   public void testRegularRemoteQuery() {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, true, null, ENTITY_USER);
      for (int i = 0; i < 50; i++) {
         remoteCache.put(i, createUser(i, i % 7));
      }

      Query<User> query = remoteCache.query("FROM sample_bank_account.User WHERE name = 'Yolka-00003' order by id");
      List<User> list = query.execute().list();
      assertThat(list).extracting("id").containsExactly(3, 10, 17, 24, 31, 38, 45);

      Map<String, Object> params = Map.of("name", "Yolka-00003");
      String output = remoteCache.execute("remote-query-access-with-stats", params);
      Json json = Json.read(output);

      Json serverTaskInfo = json.at("server-task");
      assertThat(serverTaskInfo.at("query-result-size").asInteger()).isEqualTo(7);
      assertThat(serverTaskInfo.at("param-name").asString()).isEqualTo("Yolka-00003");
      assertThat(serverTaskInfo.at("projection-query-result").asJsonList())
            .extracting(val -> val.asJsonList().get(0).asInteger())
            .containsExactly(3, 3, 10, 10, 17, 17, 24, 24, 31, 31, 38, 38, 45, 45);

      Json queryStatistics = json.at("query");
      assertThat(queryStatistics.at("indexed_local").at("count").asInteger()).isEqualTo(9); // 3 * 3
      assertThat(queryStatistics.at("indexed_distributed").at("count").asInteger()).isEqualTo(3);
      assertThat(queryStatistics.at("hybrid").at("count").asInteger()).isZero();
      assertThat(queryStatistics.at("non_indexed").at("count").asInteger()).isZero();
      assertThat(queryStatistics.at("entity_load").at("count").asInteger()).isEqualTo(2);

      query = remoteCache.query("FROM sample_bank_account.User WHERE name = 'Yolka-00003' order by id");
      list = query.execute().list();
      assertThat(list).extracting("id").containsExactly(3, 3, 10, 10, 17, 17, 24, 24, 31, 31, 38, 38, 45, 45);
   }

   public static User createUser(int id, int seed) {
      String seedPadded = String.format("%05d", seed);
      User user = new User();
      user.setId(id);
      user.setName("Yolka-" + seedPadded);
      user.setSurname("Mascotte-" + seedPadded);
      user.setGender(User.Gender.MALE);
      user.setAccountIds(Collections.singleton(seed));
      Address address = new Address();
      address.setStreet("White Alley");
      address.setPostCode(seed + "");
      user.setAddresses(Collections.singletonList(address));
      return user;
   }
}
