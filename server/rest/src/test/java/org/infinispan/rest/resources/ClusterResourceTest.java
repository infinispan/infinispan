package org.infinispan.rest.resources;

import static org.infinispan.client.rest.configuration.Protocol.HTTP_11;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_20;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.ClusterResourceTest")
public class ClusterResourceTest extends AbstractRestResourceTest {

   @Override
   public Object[] factory() {
      return new Object[] {
            new ClusterResourceTest().withSecurity(false).protocol(HTTP_11).ssl(false),
            new ClusterResourceTest().withSecurity(true).protocol(HTTP_20).ssl(false),
            new ClusterResourceTest().withSecurity(true).protocol(HTTP_11).ssl(true),
            new ClusterResourceTest().withSecurity(true).protocol(HTTP_20).ssl(true),
      };
   }

   @Test
   public void testClusterDistribution() {
      CompletionStage<RestResponse> response = adminClient.cluster().distribution();
      assertThat(response).isOk();

      Json json = Json.read(join(response).getBody());
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
}
