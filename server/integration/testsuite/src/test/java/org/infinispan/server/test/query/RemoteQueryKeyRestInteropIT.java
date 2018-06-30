package org.infinispan.server.test.query;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.server.test.category.Queries;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Tests for interoperability with REST and Hot Rod clients when using protobuf.
 */
@Category(Queries.class)
@RunWith(Arquillian.class)
public class RemoteQueryKeyRestInteropIT extends RemoteQueryBaseIT {

   @InfinispanResource("remote-query-1")
   protected RemoteInfinispanServer server;
   private final CloseableHttpClient restClient;
   private final ObjectMapper objectMapper;

   public RemoteQueryKeyRestInteropIT() {
      super("clustered", "disttestcache");
      restClient = HttpClients.createDefault();
      objectMapper = new ObjectMapper();
   }

   @Override
   protected RemoteInfinispanServer getServer() {
      return server;
   }

   @Test
   public void testHotRodRestInterop() throws Exception {
      // Write data Hot Rod 
      remoteCache.put(1, createUser1());

      // Read via Rest
      HttpGet get = new HttpGet("http://localhost:8080/rest/disttestcache/1");
      get.addHeader("Accept", APPLICATION_JSON_TYPE);
      get.addHeader("Key-Content-Type", "application/x-java-object; type=java.lang.Integer");
      HttpResponse getResponse = restClient.execute(get);
      assertEquals(HttpStatus.SC_OK, getResponse.getStatusLine().getStatusCode());
      JsonNode node = objectMapper.readTree(EntityUtils.toString(getResponse.getEntity()));

      assertEquals("sample_bank_account.User", node.get("_type").asText());
      assertEquals(1, node.get("id").asInt());
      assertEquals("John", node.get("name").asText());

      // Write via REST
      HttpPost httpPost = new HttpPost("http://localhost:8080/rest/disttestcache/2");
      httpPost.addHeader("Content-Type", APPLICATION_JSON_TYPE);
      httpPost.addHeader("Key-Content-Type", "application/x-java-object; type=java.lang.Integer");
      ObjectNode user2 = objectMapper.createObjectNode();
      user2.put("_type", "sample_bank_account.User");
      user2.put("id", 2);
      user2.put("name", "Donald");
      user2.put("surname", "Duck");
      httpPost.setEntity(new StringEntity(user2.toString()));
      CloseableHttpResponse response = restClient.execute(httpPost);
      assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());

      // Read via Hot Rod
      User user = remoteCache.get(2);
      assertEquals(2, user.getId());
      assertEquals("Donald", user.getName());
      assertEquals("Duck", user.getSurname());
   }


   @After
   public void tearDown() {
      super.tearDown();
      try {
         restClient.close();
      } catch (IOException ignored) {
      }
   }

   private User createUser1() {
      User user = new User();
      user.setId(1);
      user.setName("John");
      user.setSurname("Doe");
      user.setGender(User.Gender.MALE);
      return user;
   }
}
