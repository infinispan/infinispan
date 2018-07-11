package org.infinispan.server.test.jmx.management;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(Arquillian.class)
@WithRunningServer({@RunningServer(name = "jmx-jolokia")})
public class JmxManagementJolokiaIT {

   /*
    * The jolokia port number is configured in resources/agent.conf
    */
   private static final String JOLOKIA_URL = "http://localhost:8779/jolokia";

   private static HttpClient httpClient;

   @BeforeClass
   public static void setUp() throws Exception {

      httpClient = HttpClientBuilder.create().build();
   }

   @Test
   public void testJolokiaVersion() throws IOException {

      HttpGet get = createHttpGet("/version");

      HttpResponse response = httpClient.execute(get);

      assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
   }

   @Test
   public void testJolokiaMemoryAccess() throws IOException {

      HttpGet get = createHttpGet("/read/java.lang:type=Memory/HeapMemoryUsage");

      HttpResponse response = httpClient.execute(get);

      assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
   }

   private HttpGet createHttpGet(String relativeUrl) {

      return new HttpGet(JOLOKIA_URL + relativeUrl);
   }

}
