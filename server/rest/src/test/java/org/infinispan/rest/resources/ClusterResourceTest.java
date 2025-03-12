package org.infinispan.rest.resources;

import static org.infinispan.client.rest.configuration.Protocol.HTTP_11;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_20;
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
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.ClusterResourceTest")
public class ClusterResourceTest extends AbstractRestResourceTest {

   @Override
   public Object[] factory() {
      return new Object[] {
            new ClusterResourceTest().withSecurity(false).protocol(HTTP_11).ssl(false).browser(false),
            new ClusterResourceTest().withSecurity(false).protocol(HTTP_11).ssl(false).browser(true),
            new ClusterResourceTest().withSecurity(true).protocol(HTTP_20).ssl(false).browser(false),
            new ClusterResourceTest().withSecurity(true).protocol(HTTP_20).ssl(false).browser(true),
            new ClusterResourceTest().withSecurity(true).protocol(HTTP_11).ssl(true).browser(false),
            new ClusterResourceTest().withSecurity(true).protocol(HTTP_11).ssl(true).browser(true),
            new ClusterResourceTest().withSecurity(true).protocol(HTTP_20).ssl(true).browser(false),
            new ClusterResourceTest().withSecurity(true).protocol(HTTP_20).ssl(true).browser(true),
      };
   }

   @Test
   public void testClusterDistribution() {
      CompletionStage<RestResponse> response = adminClient.cluster().distribution();
      assertThat(response).isOk();

      Json json = Json.read(join(response).body());
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
   public void testClusterMembership() {
      RestResponse response = join(adminClient.cluster().clusterMembers());
      String cmVersion = cacheManagers.get(0).getCacheManagerInfo().getVersion();
      ResponseAssertion.assertThat(response).isOk();
      Json clusterMembership = Json.read(response.body());
      List<Json> members = clusterMembership.at(MEMBERS).asJsonList();
      assertEquals(2, members.size());
      assertTrue(members.get(0).at(NODE_ADDRESS).asString().contains("ClusterResourceTest"));
      assertEquals(cmVersion, members.get(0).at(VERSION).asString());
      assertEquals("RUNNING", members.get(0).at(CACHE_MANAGER_STATUS).asString());
      assertFalse(members.get(0).at(PHYSICAL_ADDRESSES).asString().isEmpty());
      assertFalse(clusterMembership.at(ROLLING_UPGRADE).asBoolean());
   }
}
