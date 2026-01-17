package org.infinispan.client.hotrod.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.telemetry.SpanCategory;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.Testing;
import org.infinispan.testing.annotation.TestForIssue;
import org.testng.annotations.Test;

@Test(groups = "tracing", testName = "org.infinispan.client.hotrod.tracing.TracingAttributeChangeTest")
@TestForIssue(jiraKey = "ISPN-16667")
public class TracingAttributeChangeTest extends SingleHotRodServerTest {

   private final String persistentLocation = Testing.tmpDirectory(getClass());

   private static final String CACHE_NAME = "bla";

   private static final String CACHE_DEFINITION =
         "<?xml version=\"1.0\"?>\n" +
         "<local-cache name=\"bla\" statistics=\"true\">\n" +
         "  <encoding media-type=\"application/x-protostream\"/>\n" +
         "</local-cache>";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Util.recursiveFileRemove(persistentLocation);
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder().nonClusteredDefault();
      global.globalState().enable().persistentLocation(persistentLocation)
            .configurationStorage(ConfigurationStorage.OVERLAY);
      return TestCacheManagerFactory.createServerModeCacheManager(global);
   }

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      return HotRodClientTestingUtil.startHotRodServer(cacheManager, serverBuilder);
   }

   @Test
   public void tryToChangeTracing() {
      RemoteCache<Object, Object> myCache = remoteCacheManager.administration()
            .createCache(CACHE_NAME, new StringConfiguration(CACHE_DEFINITION));
      assertThat(myCache).isNotNull();

      remoteCacheManager.administration()
            .updateConfigurationAttribute(CACHE_NAME, "tracing.enabled", "true");
      remoteCacheManager.administration()
            .updateConfigurationAttribute(CACHE_NAME, "tracing.categories", "cluster container");

      Configuration configuration = cacheManager.getCacheConfiguration(CACHE_NAME);
      assertThat(configuration.tracing().categories())
            .containsExactlyInAnyOrder(SpanCategory.CONTAINER, SpanCategory.CLUSTER);
   }
}
