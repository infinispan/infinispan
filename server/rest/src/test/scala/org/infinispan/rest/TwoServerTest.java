/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.rest;

import static org.testng.Assert.*;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.infinispan.api.BasicCacheContainer;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
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
public class TwoServerTest extends RESTServerTestBase {

   private static final String PATH1 = "http://localhost:8890/rest/___defaultcache/";
   private static final String PATH2 = "http://localhost:8891/rest/___defaultcache/";

   @BeforeClass
   private void setUp() throws Exception {
      ConfigurationBuilder cfgBuilder = AbstractCacheTest.getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cfgBuilder.transaction().transactionManagerLookup(new DummyTransactionManagerLookup());
      cfgBuilder.clustering().hash().numOwners(2);
      cfgBuilder.clustering().stateTransfer().fetchInMemoryState(true);
      cfgBuilder.clustering().stateTransfer().timeout(20000);
      RestServerConfigurationBuilder restCfgBuilder = new RestServerConfigurationBuilder();
      addServer("1", 8890, TestCacheManagerFactory.createClusteredCacheManager(cfgBuilder), restCfgBuilder.build());
      addServer("2", 8891, TestCacheManagerFactory.createClusteredCacheManager(cfgBuilder), restCfgBuilder.build());
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
}
