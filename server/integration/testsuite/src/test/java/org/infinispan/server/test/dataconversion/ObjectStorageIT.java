package org.infinispan.server.test.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.junit.Assert.assertEquals;

import java.io.File;
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
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.server.test.util.ITestUtils;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Test for server storing java objects with deployed entities.
 *
 * @since 9.4
 */
@RunWith(Arquillian.class)
public class ObjectStorageIT {

   static final String DEPLOY_PATH = "/standalone/deployments/entities.jar";

   static final String SERVER = "standalone-pojo-storage";

   static final ObjectMapper mapper = new ObjectMapper();

   @InfinispanResource(SERVER)
   RemoteInfinispanServer server;
   private static CloseableHttpClient httpClient;

   @BeforeClass
   public static void before() {
      httpClient = HttpClients.createDefault();
      String serverDir = System.getProperty("server1.dist");

      JavaArchive jar = ShrinkWrap.create(JavaArchive.class).addClass(Currency.class)
            .add(new StringAsset("Dependencies: org.infinispan.commons"), "META-INF/MANIFEST.MF");

      File f = new File(serverDir, DEPLOY_PATH);
      jar.as(ZipExporter.class).exportTo(f, true);
   }

   @AfterClass
   public static void cleanUp() {
      try {
         httpClient.close();
      } catch (IOException ignored) {
      }
      String serverDir = System.getProperty("server1.dist");
      File jar = new File(serverDir, DEPLOY_PATH);
      if (jar.exists())
         jar.delete();

      File f = new File(serverDir, DEPLOY_PATH + ".deployed");
      if (f.exists())
         f.delete();
   }

   @Test
   @WithRunningServer({@RunningServer(name = SERVER)})
   public void shouldDeserializeStringContent() throws IOException {
      RemoteCacheManager rcm = ITestUtils.createCacheManager(server);
      RemoteCache<Integer, Currency> remoteCache = rcm.getCache();
      remoteCache.put(1, new Currency("United States", "USD"));
      remoteCache.put(2, new Currency("Algeria", "DZD"));

      JsonNode jsonNode = readJSON(1);
      assertEquals("United States", jsonNode.get("country").asText());
      assertEquals("USD", jsonNode.get("symbol").asText());

      JsonNode anotherNode = readJSON(2);
      assertEquals(Currency.class.getName(), anotherNode.get("_type").asText());

      String newCurrency = "{\"_type\":\"org.infinispan.server.test.dataconversion.Currency\",\"country\":\"Bulgaria\",\"symbol\":\"BGN\"}";
      writeViaRest(3, newCurrency, APPLICATION_JSON);
      Currency bgn = remoteCache.get(3);
      assertEquals("Bulgaria", bgn.getCountry());
      assertEquals("BGN", bgn.getSymbol());

      String newCurrencyXML = "<org.infinispan.server.test.dataconversion.Currency>" +
            "<country>Mexico</country>" +
            "<symbol>MXN</symbol>" +
            "</org.infinispan.server.test.dataconversion.Currency>";

      writeViaRest(4, newCurrencyXML, MediaType.APPLICATION_XML);
      Currency mxn = remoteCache.get(4);
      assertEquals("Mexico", mxn.getCountry());
      assertEquals("MXN", mxn.getSymbol());
   }

   private void writeViaRest(Integer key, String newCurrency, MediaType valueFormat) throws IOException {
      HttpPost httpPost = new HttpPost(getURL(key));
      httpPost.addHeader("Content-Type", valueFormat.toString());
      httpPost.addHeader("Key-Content-Type", "application/x-java-object; type=java.lang.Integer");
      httpPost.setEntity(new StringEntity(newCurrency));
      CloseableHttpResponse response = httpClient.execute(httpPost);
      assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
   }

   private String getURL(Integer key) {
      return "http://localhost:8080/rest/default/" + key;
   }

   private JsonNode readJSON(Integer key) throws IOException {
      String json = readViaRest(key, MediaType.APPLICATION_JSON);
      return mapper.readTree(json);
   }

   private String readViaRest(Integer key, MediaType valueFormat) throws IOException {
      HttpGet get = new HttpGet(getURL(key));
      get.addHeader("Accept", valueFormat.toString());
      get.addHeader("Key-Content-Type", "application/x-java-object; type=java.lang.Integer");
      HttpResponse getResponse = httpClient.execute(get);
      assertEquals(HttpStatus.SC_OK, getResponse.getStatusLine().getStatusCode());
      return EntityUtils.toString(getResponse.getEntity());
   }


}
