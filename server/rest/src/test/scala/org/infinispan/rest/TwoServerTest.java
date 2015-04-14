package org.infinispan.rest;

import static org.junit.Assert.assertEquals;
import static org.testng.Assert.*;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test with two REST Servers.
 *
 * @author Michal Linhard (mlinhard@redhat.com)
 */
@Test(groups = { "functional" }, testName = "rest.TwoServerTest")
public class TwoServerTest extends RestServerTestBase {

   private static final String PATH1 = "http://localhost:8890/rest/___defaultcache/";
   private static final String PATH2 = "http://localhost:8891/rest/___defaultcache/";

   private static final String EXPIRY_PATH1 = "http://localhost:8890/rest/expiry/";
   private static final String EXPIRY_PATH2 = "http://localhost:8891/rest/expiry/";

   @BeforeClass
   private void setUp() throws Exception {
      ConfigurationBuilder cfgBuilder = AbstractCacheTest.getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cfgBuilder.transaction().transactionManagerLookup(new DummyTransactionManagerLookup());
      cfgBuilder.clustering().hash().numOwners(2);
      cfgBuilder.clustering().stateTransfer().fetchInMemoryState(true);
      cfgBuilder.clustering().stateTransfer().timeout(20000);

      ConfigurationBuilder expiryCfgBuilder = AbstractCacheTest.getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      expiryCfgBuilder.expiration().lifespan(2000).maxIdle(2000);

      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(cfgBuilder);
      cm1.defineConfiguration("expiry", expiryCfgBuilder.build());
      addServer("1", cm1, new RestServerConfigurationBuilder().port(8890).build());

      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(cfgBuilder);
      cm2.defineConfiguration("expiry", expiryCfgBuilder.build());
      addServer("2", cm2, new RestServerConfigurationBuilder().port(8891).build());

      startServers();
      TestingUtil.blockUntilViewsReceived(10000, getCacheManager("1").getCache(BasicCacheContainer.DEFAULT_CACHE_NAME),
         getCacheManager("2").getCache(BasicCacheContainer.DEFAULT_CACHE_NAME));
      createClient();
   }

   @AfterClass
   private void tearDown() throws Exception {
      stopServers();
      destroyClient();
   }

   public void testPutReplication() throws Exception {
      PutMethod put = new PutMethod(PATH1 + "a");
      put.setRequestEntity(new StringRequestEntity("data", "application/text", null));
      call(put);
      assertEquals(put.getStatusCode(), HttpServletResponse.SC_OK);
      put.releaseConnection();
      GetMethod get = new GetMethod(PATH1 + "a");
      call(get);
      assertEquals(get.getStatusCode(), HttpServletResponse.SC_OK);
      get.releaseConnection();
      get = new GetMethod(PATH2 + "a");
      call(get);
      assertEquals(get.getStatusCode(), HttpServletResponse.SC_OK);
      assertEquals("data", get.getResponseBodyAsString());
      get.releaseConnection();
   }

   public void testReplace() throws Exception {
      PutMethod put = new PutMethod(PATH1 + "testReplace");
      put.setRequestEntity(new StringRequestEntity("data", "application/text", null));
      call(put);
      assertEquals(put.getStatusCode(), HttpServletResponse.SC_OK);
      put.releaseConnection();
      put = new PutMethod(PATH1 + "testReplace");
      put.setRequestEntity(new StringRequestEntity("data2", "application/text", null));
      call(put);
      assertEquals(put.getStatusCode(), HttpServletResponse.SC_OK);
      put.releaseConnection();
      GetMethod get = new GetMethod(PATH2 + "testReplace");
      call(get);
      assertEquals(get.getStatusCode(), HttpServletResponse.SC_OK);
      assertEquals("data2", get.getResponseBodyAsString());
      get.releaseConnection();
   }

   public void testExtendedHeaders() throws Exception {
      PutMethod put = new PutMethod(PATH1 + "testExtendedHeaders");
      put.setRequestEntity(new StringRequestEntity("data", "application/text", null));
      call(put);
      assertEquals(put.getStatusCode(), HttpServletResponse.SC_OK);
      put.releaseConnection();
      GetMethod get = new GetMethod(PATH2 + "testExtendedHeaders?extended");
      call(get);
      assertEquals(get.getStatusCode(), HttpServletResponse.SC_OK);
      Header po = get.getResponseHeader("Cluster-Primary-Owner");
      assertNotNull(po);
      Address primaryLocation = getCacheManager("1").getCache(BasicCacheContainer.DEFAULT_CACHE_NAME).getAdvancedCache().getDistributionManager().getPrimaryLocation("testExtendedHeaders");
      assertEquals(primaryLocation.toString(), po.getValue());
      Header sa = get.getResponseHeader("Cluster-Server-Address");
      assertNotNull(sa);
      JGroupsTransport transport = (JGroupsTransport)getCacheManager("2").getTransport();
      assertEquals(transport.getPhysicalAddresses().toString(), sa.getValue());
      Header nn = get.getResponseHeader("Cluster-Node-Name");
      assertNotNull(nn);
      assertEquals(transport.getAddress().toString(), nn.getValue());
      get.releaseConnection();
   }

   public void testExpiration() throws Exception {
      String key1Path = EXPIRY_PATH1 + "k1";
      String key2Path = EXPIRY_PATH2 + "k2";
      String key3Path = EXPIRY_PATH1 + "k3";
      String key4Path = EXPIRY_PATH1 + "k4";
      // specific entry timeToLiveSeconds and maxIdleTimeSeconds that overrides the default
      post(key1Path, "v1", "application/text", HttpServletResponse.SC_OK, "Content-Type", "application/text",
         "timeToLiveSeconds", "3", "maxIdleTimeSeconds", "3");
      // no value means never expire
      post(key2Path, "v2", "application/text", HttpServletResponse.SC_OK, "Content-Type", "application/text");
      // 0 value means use default
      post(key3Path, "v3", "application/text", HttpServletResponse.SC_OK, "Content-Type", "application/text",
         "timeToLiveSeconds", "0", "maxIdleTimeSeconds", "0");
      post(key4Path, "v4", "application/text", HttpServletResponse.SC_OK, "Content-Type", "application/text",
         "timeToLiveSeconds", "0", "maxIdleTimeSeconds", "2");

      TestingUtil.sleepThread(1000);
      get(key1Path, "v1");
      get(key3Path, "v3");
      get(key4Path, "v4");
      TestingUtil.sleepThread(1100);
      // k3 and k4 expired
      get(key1Path, "v1");
      head(key3Path, HttpServletResponse.SC_NOT_FOUND);
      head(key4Path, HttpServletResponse.SC_NOT_FOUND);
      TestingUtil.sleepThread(1000);
      // k1 expired
      head(key1Path, HttpServletResponse.SC_NOT_FOUND);
      // k2 should not be expired because without timeToLive/maxIdle parameters,
      // the entries live forever. To use default values, 0 must be passed in.
      head(key2Path, HttpServletResponse.SC_OK);
   }

   private void post(String uri, String data, String contentType, int expectedCode, Object... headers) throws Exception {
      PostMethod post = new PostMethod(uri);
      for (int i = 0; i < headers.length; i += 2)
         post.setRequestHeader(headers[i].toString(), headers[i + 1].toString());

      post.setRequestEntity(new StringRequestEntity(data, contentType, null));
      call(post);
      assertEquals(expectedCode, post.getStatusCode());
   }

   private void get(String uri, String expectedResponseBody) throws Exception {
      GetMethod get = new GetMethod(uri);
      call(get);
      assertEquals(expectedResponseBody, get.getResponseBodyAsString());
   }

   private void head(String uri, int expectedCode) throws Exception {
      HeadMethod head = new HeadMethod(uri);
      call(head);
      assertEquals(expectedCode, head.getStatusCode());
   }

}
