package org.infinispan.server.test.client.router;

import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.server.test.category.Security;
import org.infinispan.server.test.client.rest.RESTHelper;
import org.infinispan.server.test.util.security.SecurityConfigurationHelper;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests Multi-tenancy feature. HotRod client performs write operation and read goes through REST. Note, that
 * compatibility mode is a must here.
 * <p>
 *     Since this test is pretty slow (requires booting up full server with Arquillian), it contains
 *     only high level tests. For more complicated scenarios, see tests from {@link org.infinispan.server.router.integration}.
 * </p>
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
@RunWith(Arquillian.class)
@Category({Security.class})
@WithRunningServer({@RunningServer(name = "hotrodSslWithMultitenancy", config = "testsuite/hotrod-ssl-with-multitenancy.xml")})
public class MultitenancyIT {

   public static final String CACHE_NAME = "cache-1";

   private static RemoteCache<String, Object> remoteCache = null;
   private static RemoteCacheManager remoteCacheManager = null;
   RESTHelper rest;

   @After
   public void release() {
      if (remoteCacheManager != null) {
         remoteCacheManager.stop();
      }
      rest.clearServers();
   }

   @Before
   public void init() {
      ConfigurationBuilder builder = new SecurityConfigurationHelper().withDefaultSsl().withSni("sni1");
      //FIXME: Use Infinispan Arquillian support after implementing https://issues.jboss.org/browse/ARQ-2035
      builder.addServer().host("127.0.0.1").port(11222);
      remoteCacheManager = new RemoteCacheManager(builder.build());
      remoteCache = remoteCacheManager.getCache(CACHE_NAME);

      rest = new RESTHelper();
      rest.addServer("127.0.0.1", 8080, "/rest/multi-tenancy-1");
   }

   @Test
   public void testWritesThroughHotrodAndReadsThroughREST() throws Exception {
      //when
      remoteCache.put("hello", "Infinispan!");

      //then
      rest.get(rest.fullPathKey(CACHE_NAME, "hello"), "Infinispan!", 200, true, "Accept", "text/plain");
   }

}
