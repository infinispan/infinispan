package org.infinispan.server.test.client.rest;

import static org.jgroups.util.Util.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.net.URI;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpOptions;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.RESTSingleNode;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Test a custom REST client connected to a single Infinispan server.
 * The server is running in standalone mode.
 *
 * @author mgencur
 */
@RunWith(Arquillian.class)
@Category({RESTSingleNode.class})
public class RESTClientIT extends AbstractRESTClientIT {

   @InfinispanResource("container1")
   RemoteInfinispanServer server1;

   @Override
   protected void addRestServer() {
      rest.addServer(server1.getRESTEndpoint().getInetAddress().getHostName(), server1.getRESTEndpoint().getContextPath());
   }

   @Test
   public void testSimpleCORSRequest() throws Exception {
      URI fullPathKey = rest.fullPathKey("cors_key");

      HttpGet get = new HttpGet(fullPathKey);
      get.addHeader("Origin", "http://host1");
      HttpResponse resp = rest.client.execute(get);

      Header allowOriginHeader = resp.getFirstHeader("access-control-allow-origin");
      assertNotNull(allowOriginHeader);
      assertEquals("http://host1", allowOriginHeader.getValue());
   }

   @Test
   public void testPreflightCORSRequest() throws Exception {
      URI fullPathKey = rest.fullPathKey("cors_preflight_key");
      HttpOptions options = new HttpOptions(fullPathKey);
      options.addHeader("Origin", "http://whatever");
      options.addHeader("Access-Control-Request-Method", "PUT");
      options.addHeader("Access-Control-Request-Headers", "Key-Content-Type");
      HttpResponse resp = rest.client.execute(options);

      assertEquals("*", resp.getFirstHeader("access-control-allow-origin").getValue());
      String value = resp.getFirstHeader("access-control-allow-methods").getValue();
      assertTrue(value.contains("GET"));
      assertTrue(value.contains("POST"));
      assertTrue(value.contains("PUT"));
      assertEquals("Key-Content-Type", resp.getFirstHeader("access-control-allow-headers").getValue());
   }
}
