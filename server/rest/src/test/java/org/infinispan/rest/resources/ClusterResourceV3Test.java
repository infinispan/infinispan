package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCEPT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.infinispan.rest.resources.ClusterResource.CACHE_MANAGER_STATUS;
import static org.infinispan.rest.resources.ClusterResource.MEMBERS;
import static org.infinispan.rest.resources.ClusterResource.NODE_ADDRESS;
import static org.infinispan.rest.resources.ClusterResource.PHYSICAL_ADDRESSES;
import static org.infinispan.rest.resources.ClusterResource.ROLLING_UPGRADE;
import static org.infinispan.rest.resources.ClusterResource.VERSION;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.testng.annotations.Test;

/**
 * Tests for REST v3 Cluster API endpoints.
 *
 * @since 16.0
 */
@Test(groups = "functional", testName = "rest.ClusterResourceV3Test")
public class ClusterResourceV3Test extends AbstractRestResourceTest {

   @Override
   public Object[] factory() {
      return new Object[]{
            new ClusterResourceV3Test().withSecurity(false).browser(false),
            new ClusterResourceV3Test().withSecurity(false).browser(true),
            new ClusterResourceV3Test().withSecurity(true).browser(false),
            new ClusterResourceV3Test().withSecurity(true).browser(true),
      };
   }

   @Test
   public void testClusterDistribution() {
      RestResponse response = join(adminClient.raw().get("/rest/v3/cluster/_distribution",
            Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));
      assertThat(response).isOk();

      Json json = Json.read(response.body());
      assertTrue(json.isArray());
      List<Json> list = json.asJsonList();

      assertEquals(NUM_SERVERS, list.size());
      Pattern pattern = Pattern.compile(this.getClass().getSimpleName() + "-Node[a-zA-Z]$");
      for (Json node : list) {
         assertTrue(node.at("memory_available").asLong() > 0);
         assertTrue(node.at("memory_used").asLong() > 0);
         assertEquals(node.at("node_addresses").asJsonList().size(), 1);
         assertTrue(pattern.matcher(node.at("node_name").asString()).matches());
      }
   }

   @Test
   public void testClusterMembers() {
      RestResponse response = join(adminClient.raw().get("/rest/v3/cluster/members",
            Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));
      assertThat(response).isOk();

      String cmVersion = cacheManagers.get(0).getCacheManagerInfo().getVersion();
      Json clusterMembership = Json.read(response.body());
      List<Json> members = clusterMembership.at(MEMBERS).asJsonList();
      assertEquals(2, members.size());
      assertTrue(members.get(0).at(NODE_ADDRESS).asString().contains("ClusterResourceV3Test"));
      assertEquals(cmVersion, members.get(0).at(VERSION).asString());
      assertEquals("RUNNING", members.get(0).at(CACHE_MANAGER_STATUS).asString());
      assertFalse(members.get(0).at(PHYSICAL_ADDRESSES).asString().isEmpty());
      assertFalse(clusterMembership.at(ROLLING_UPGRADE).asBoolean());
   }

   @Test
   public void testOpenAPIIncludesClusterV3() {
      RestResponse response = join(client.raw().get("/rest/v3/openapi",
            Map.of(ACCEPT.toString(), APPLICATION_JSON_TYPE)));
      assertThat(response).isOk();

      String body = response.body();
      Json spec = Json.read(body);

      // Verify OpenAPI version
      assertEquals("3.1.1", spec.at("openapi").asString());

      // Verify paths exist
      Json paths = spec.at("paths");
      assertTrue(paths.has("/rest/v3/cluster/_stop"));
      assertTrue(paths.has("/rest/v3/cluster/_distribution"));
      assertTrue(paths.has("/rest/v3/cluster/members"));
      assertTrue(paths.has("/rest/v3/cluster/backups"));
      assertTrue(paths.has("/rest/v3/cluster/restores"));
      assertTrue(paths.has("/rest/v3/cluster/raft"));

      // Verify operationIds
      Json stopOp = paths.at("/rest/v3/cluster/_stop").at("post");
      assertEquals("stopCluster", stopOp.at("operationId").asString());

      Json distributionOp = paths.at("/rest/v3/cluster/_distribution").at("get");
      assertEquals("getClusterDistribution", distributionOp.at("operationId").asString());

      Json membersOp = paths.at("/rest/v3/cluster/members").at("get");
      assertEquals("getClusterMembers", membersOp.at("operationId").asString());
   }
}
