package org.infinispan.it.endpoints;


import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.startHotRodServer;
import static org.infinispan.rest.JSONConstants.TYPE;
import static org.infinispan.server.core.test.ServerTestingUtil.findFreePort;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.infinispan.commons.util.concurrent.CompletionStages.join;
import static org.testng.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.server.core.DummyServerManagement;
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
   RestClient restClient;
   private EmbeddedCacheManager cacheManager;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<String, CryptoCurrency> remoteCache;

   private static final String CACHE_NAME = "indexed";

   HotRodServer hotRodServer;

   private RestCacheClient restCacheClient;

   abstract ConfigurationBuilder getIndexCacheConfiguration();

   abstract RemoteCacheManager createRemoteCacheManager() throws Exception;

   @BeforeClass
   protected void setup() throws Exception {
      cacheManager = TestCacheManagerFactory.createServerModeCacheManager(EndpointITSCI.INSTANCE, new ConfigurationBuilder());
      cacheManager.getClassAllowList().addRegexps(".*");
      cacheManager.defineConfiguration(CACHE_NAME, getIndexCacheConfiguration().build());

      RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder();

      int restPort = findFreePort();
      builder.port(restPort);

      restServer = new RestServer();
      restServer.setServerManagement(new DummyServerManagement(), true);
      restServer.start(builder.build(), cacheManager);
      restServer.postStart();
      restClient = RestClient.forConfiguration(new RestClientConfigurationBuilder().addServer().host(restServer.getHost()).port(restServer.getPort()).build());
      restCacheClient = restClient.cache(CACHE_NAME);
      hotRodServer = startHotRodServer(cacheManager);
      remoteCacheManager = createRemoteCacheManager();
      remoteCache = remoteCacheManager.getCache(CACHE_NAME);
   }

   protected String getEntityName() {
      return EndpointITSCI.getFQN(CryptoCurrency.class);
   }

   protected String getJsonType() {
      return getEntityName();
   }

   private void writeCurrencyViaJson(String key, String description, int rank) {
      Json currency = Json.object();
      currency.set(TYPE, getJsonType());
      currency.set("description", description);
      currency.set("rank", rank);
      RestEntity value = RestEntity.create(MediaType.APPLICATION_JSON, currency.toString());
      RestResponse response = join(restCacheClient.put(key, value));

      assertEquals(response.status(), 204);
   }

   private CryptoCurrency readCurrencyViaJson(String key) {
      RestResponse response = join(restCacheClient.get(key, MediaType.APPLICATION_JSON_TYPE));
      String json = response.body();
      Json jsonNode = Json.read(json);
      Json description = jsonNode.at("description");
      Json rank = jsonNode.at("rank");
      return new CryptoCurrency(description.asString(), rank.asInteger());
   }

   @Test
   public void testRestOnly() {
      writeCurrencyViaJson("DASH", "Dash", 7);
      writeCurrencyViaJson("IOTA", "Iota", 8);
      writeCurrencyViaJson("XMR", "Monero", 9);

      CryptoCurrency xmr = readCurrencyViaJson("XMR");

      assertEquals(xmr.getRank(), Integer.valueOf(9));
      assertEquals(xmr.getDescription(), "Monero");
   }

   @Test
   public void testHotRodInteroperability() {
      remoteCache.clear();
      // Put object via Hot Rod
      remoteCache.put("BTC", new CryptoCurrency("Bitcoin", 1));
      remoteCache.put("ETH", new CryptoCurrency("Ethereum", 2));
      remoteCache.put("XRP", new CryptoCurrency("Ripple", 3));
      remoteCache.put("CAT", new CryptoCurrency("Catcoin", 618));

      assertEquals(remoteCache.get("CAT").getDescription(), "Catcoin");
      assertEquals(remoteCache.size(), 4);

      Query<CryptoCurrency> query = remoteCache.query("FROM " + getEntityName() + " c where c.rank < 10");
      List<CryptoCurrency> highRankCoins = query.execute().list();
      assertEquals(highRankCoins.size(), 3);

      // Read as Json
      CryptoCurrency btc = readCurrencyViaJson("BTC");
      assertEquals(btc.getDescription(), "Bitcoin");
      assertEquals(btc.getRank(), Integer.valueOf(1));

      // Write as Json
      writeCurrencyViaJson("LTC", "Litecoin", 4);

      // Assert inserted entity is searchable
      query = remoteCache.query("FROM " + getEntityName() + " c  where c.description = 'Litecoin'");

      CryptoCurrency litecoin = query.execute().list().iterator().next();
      assertEquals(litecoin.getDescription(), "Litecoin");
      assertEquals(litecoin.getRank(), Integer.valueOf(4));

      // Read as JSON from the Hot Rod client
      Object jsonResult = remoteCache.withDataFormat(DataFormat.builder().valueType(MediaType.APPLICATION_JSON).build()).get("LTC");

      Json jsonNode = Json.read(new String((byte[]) jsonResult, StandardCharsets.UTF_8));
      assertEquals(jsonNode.at("description").asString(), "Litecoin");
   }

   @AfterClass
   protected void teardown() {
      Util.close(restClient);
      remoteCacheManager.stop();
      if (restServer != null) {
         try {
            restServer.stop();
         } catch (Exception ignored) {
         }
      }
      killCacheManagers(cacheManager);
      cacheManager = null;
      killServers(hotRodServer);
      hotRodServer = null;
   }
}
