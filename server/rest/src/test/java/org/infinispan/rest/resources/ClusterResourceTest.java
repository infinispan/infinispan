package org.infinispan.rest.resources;

import static org.testng.AssertJUnit.assertEquals;

import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Test(groups = "functional", testName = "rest.ClusterResourceTest")
public class ClusterResourceTest extends AbstractRestResourceTest {

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
   }

   @Test
   public void testCluster() throws Exception {
      String url = String.format("http://localhost:%d/rest/v2/cluster", restServer().getPort());
      ContentResponse response = client.newRequest(url).send();
      ResponseAssertion.assertThat(response).isOk();

      JsonNode jsonNode = new ObjectMapper().readTree(response.getContent());
      assertEquals(jsonNode.get("healthStatus").asText(), "HEALTHY");
      assertEquals(jsonNode.get("nodeNames").size(), 2);

      response = client.newRequest(url).method(HttpMethod.HEAD).send();
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasNoContent();
   }
}
