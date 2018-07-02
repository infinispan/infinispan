package org.infinispan.it.compatibility;


import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.startHotRodServer;
import static org.infinispan.rest.JSONConstants.TYPE;
import static org.infinispan.server.core.test.ServerTestingUtil.findFreePort;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.dsl.Query;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Base class for Json reading/writing/querying across multiple endpoints.
 *
 * @since 9.2
 */
@Test(groups = "functional")
public abstract class BaseJsonTest extends AbstractInfinispanTest {

   RestServer restServer;
   HttpClient restClient;
   private EmbeddedCacheManager cacheManager;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<String, CryptoCurrency> remoteCache;
   private static final ObjectMapper MAPPER = new ObjectMapper();

   private static final String CACHE_NAME = "indexed";

   HotRodServer hotRodServer;

   private String restEndpoint;

   abstract ConfigurationBuilder getIndexCacheConfiguration();

   abstract RemoteCacheManager createRemoteCacheManager() throws Exception;


   @BeforeClass
   protected void setup() throws Exception {
      cacheManager = TestCacheManagerFactory.createServerModeCacheManager();
      ClassWhiteList classWhiteList = cacheManager.getClassWhiteList();
      classWhiteList.addRegexps(".*");

      cacheManager.defineConfiguration(CACHE_NAME, getIndexCacheConfiguration().build());

      RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder();

      int restPort = findFreePort();
      builder.port(restPort);

      restServer = new RestServer();
      restServer.start(builder.build(), cacheManager);
      restClient = new HttpClient();

      hotRodServer = startHotRodServer(cacheManager);
      remoteCacheManager = createRemoteCacheManager();
      remoteCache = remoteCacheManager.getCache(CACHE_NAME);
      restEndpoint = String.format("http://localhost:%s/rest/%s", restServer.getPort(), CACHE_NAME);
   }

   abstract String getEntityName();

   private void writeCurrencyViaJson(String key, String description, int rank) throws IOException {
      EntityEnclosingMethod put = new PutMethod(restEndpoint + "/" + key);
      ObjectNode currency = MAPPER.createObjectNode();
      currency.put(TYPE, getEntityName());
      currency.put("description", description);
      currency.put("rank", rank);
      put.setRequestEntity(new StringRequestEntity(currency.toString(), "application/json", "UTF-8"));

      restClient.executeMethod(put);
      System.out.println(put.getResponseBodyAsString());
      assertEquals(put.getStatusCode(), HttpStatus.SC_OK);
   }

   private CryptoCurrency readCurrencyViaJson(String key) throws IOException {
      HttpMethod get = new GetMethod(restEndpoint + "/" + key);
      get.setRequestHeader("Accept", "application/json");
      restClient.executeMethod(get);
      String json = get.getResponseBodyAsString();
      JsonNode jsonNode = new ObjectMapper().readTree(json);
      JsonNode description = jsonNode.get("description");
      JsonNode rank = jsonNode.get("rank");
      return new CryptoCurrency(description.asText(), rank.getIntValue());
   }

   @Test
   public void testRestOnly() throws Exception {
      writeCurrencyViaJson("DASH", "Dash", 7);
      writeCurrencyViaJson("IOTA", "Iota", 8);
      writeCurrencyViaJson("XMR", "Monero", 9);

      CryptoCurrency xmr = readCurrencyViaJson("XMR");

      assertEquals(xmr.getRank(), Integer.valueOf(9));
      assertEquals(xmr.getDescription(), "Monero");
   }

   @Test
   public void testHotRodInteroperability() throws Exception {
      remoteCache.clear();
      // Put object via Hot Rod
      remoteCache.put("BTC", new CryptoCurrency("Bitcoin", 1));
      remoteCache.put("ETH", new CryptoCurrency("Ethereum", 2));
      remoteCache.put("XRP", new CryptoCurrency("Ripple", 3));
      remoteCache.put("CAT", new CryptoCurrency("Catcoin", 618));

      assertEquals(remoteCache.get("CAT").getDescription(), "Catcoin");
      assertEquals(remoteCache.size(), 4);

      Query query = Search.getQueryFactory(remoteCache).create("FROM " + getEntityName() + " c where c.rank < 10");
      List<CryptoCurrency> highRankCoins = query.list();
      assertEquals(highRankCoins.size(), 3);

      // Read as Json
      CryptoCurrency btc = readCurrencyViaJson("BTC");
      assertEquals("Bitcoin", btc.getDescription());
      assertEquals(Integer.valueOf(1), btc.getRank());

      // Write as Json
      writeCurrencyViaJson("LTC", "Litecoin", 4);

      // Assert inserted entity is searchable
      query = Search.getQueryFactory(remoteCache).create("FROM " + getEntityName() + " c  where c.description = 'Litecoin'");

      CryptoCurrency litecoin = (CryptoCurrency) query.list().iterator().next();
      assertEquals(litecoin.getDescription(), "Litecoin");
      assertTrue(litecoin.getRank() == 4);

      // Read as JSON from the Hot Rod client
      Object jsonResult = remoteCache.withDataFormat(DataFormat.builder().valueType(MediaType.APPLICATION_JSON).build()).get("LTC");

      JsonNode jsonNode = new ObjectMapper().readTree((byte[]) jsonResult);
      assertEquals("Litecoin", jsonNode.get("description").asText());
   }

   @AfterClass
   protected void teardown() {
      remoteCacheManager.stop();
      if (restServer != null) {
         try {
            restServer.stop();
         } catch (Exception ignored) {
         }
      }
      killCacheManagers(cacheManager);
      killServers(hotRodServer);
   }


}
