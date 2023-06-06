package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.resp.test.RespTestingUtil.OK;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.resp.ClusteredOperationsTest")
public class ClusteredOperationsTest extends BaseMultipleRespTest {

   public void testClusteredGetAndSet() {
      for (int i = 0; i < 100; i++) {
         assertThat(redisConnection1.sync().set("key" + i, "value" + i)).isEqualTo(OK);
      }


      for (int i = 0; i < 100; i++) {
         assertThat( redisConnection2.sync().get("key" + i)).isEqualTo("value" + i);
      }
   }

   public void retrieveShardsInformation() {
      validate(redisConnection1.sync().clusterShards());
      validate(redisConnection2.sync().clusterShards());
   }

   public void retrieveNodesInformation() {
      assertClusterNodesResponse(redisConnection1.sync().clusterNodes());
      assertClusterNodesResponse(redisConnection2.sync().clusterNodes());
   }

   private void assertClusterNodesResponse(String response) {
      String[] nodes = response.split("\n");
      assertThat(nodes).hasSize(2);

      for (String node : nodes) {
         String[] information = node.split(" ");
         assertThat(information)
               // Number of slots vary.
               .hasSizeGreaterThan(8)
               .containsAnyOf("master", "myself,master")
               .satisfies(c -> assertThat(Stream.of(c).anyMatch(s -> s.startsWith("127.0.0.1:"))).isTrue())
               .contains("connected", "-", "0");
      }
   }

   private void validate(List<Object> shards) {
      // We have 2 nodes in the system.
      assertThat(shards).hasSize(2);

      assertShard(asList(shards, 0), 2);
      assertShard(asList(shards, 1), 2);
   }

   static void assertShard(List<Object> values, int size) {
      assertThat(values)
            .hasSize(4)
            .contains("slots", "nodes");

      Map<String, Object> slot = new HashMap<>();
      slot.put((String) values.get(0), values.get(1));
      slot.put((String) values.get(2), values.get(3));

      assertThat(slot.get("slots")).isInstanceOf(List.class)
            .asList()
            .isNotEmpty();

      assertThat(slot.get("nodes")).isInstanceOf(List.class)
            .asList()
            .hasSize(size);

      for (int i = 0; i < size; i++) {
         // First node always master.
         String role = i == 0 ? "master" : "replica";
         assertNode(toNodeInformation(asList((List<Object>) slot.get("nodes"), i)), role);
      }
   }

   private static void assertNode(Map<String, Object> node, String role) {
      assertThat(node)
            // Some required keys from the specification.
            .containsKeys("id", "port", "endpoint", "ip", "replication-offset", "role", "health")
            .containsEntry("role", role)
            .containsEntry("replication-offset", 0L)
            .containsEntry("health", "online")
            .hasEntrySatisfying("port", port -> assertThat((Long) port).isGreaterThanOrEqualTo(0))
            .hasEntrySatisfying("endpoint", endpoint -> assertThat(endpoint).isEqualTo(node.get("ip")));
   }

   private static List<Object> asList(List<Object> slots, int idx) {
      Object object = slots.get(idx);
      assertThat(object).isInstanceOf(List.class);
      return (List<Object>) object;
   }

   private static Map<String, Object> toNodeInformation(List<Object> values) {
      assertThat(values).size()
            .satisfies(size -> assertThat(size).isEven());

      Map<String, Object> node = new HashMap<>();
      for (int i = 0; i < values.size(); i += 2) {
         node.put((String) values.get(i), values.get(i + 1));
      }
      return node;
   }
}
