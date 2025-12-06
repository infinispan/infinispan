package org.infinispan.rest.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.rest.RestResponseInfo.OK;
import static org.infinispan.client.rest.RestResponseInfo.SERVICE_UNAVAILABLE;
import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.infinispan.partitionhandling.PartitionHandling.DENY_READ_WRITES;

import java.nio.file.Paths;
import java.util.stream.Stream;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestServerClient;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@CleanupAfterMethod
@Test(groups = "functional", testName = "rest.ServerHealthCheckResourceTest")
public class HealthCheckResourceTest extends AbstractRestResourceTest {

   private static final String PERSISTENT_LOCATION = tmpDirectory(HealthCheckResourceTest.class.getName());
   private static final String TEST_CACHE_NAME = "testing-cache";

   private RestServerClient restServerClient;

   @Override
   public Object[] factory() {
      return Stream.of(Protocol.values())
            .flatMap(protocol ->
                  Stream.of(true, false)
                        .flatMap(security -> Stream.of(
                              new HealthCheckResourceTest().withSecurity(security).protocol(protocol).browser(true),
                              new HealthCheckResourceTest().withSecurity(security).protocol(protocol).browser(false)
                        )))
            .toArray(Object[]::new);
   }

   @Override
   protected GlobalConfigurationBuilder getGlobalConfigForNode(int id) {
      GlobalConfigurationBuilder config = super.getGlobalConfigForNode(id);
      config.globalState().enable()
            .configurationStorage(ConfigurationStorage.OVERLAY)
            .persistentLocation(Paths.get(PERSISTENT_LOCATION, Integer.toString(id)).toString());
      config.jmx().enabled(false);
      return config;
   }

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.statistics().enable().clustering().cacheMode(DIST_SYNC).partitionHandling().whenSplit(DENY_READ_WRITES).aliases("alias");
      cm.defineConfiguration(TEST_CACHE_NAME, builder.build());

      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.simpleCache(true).clustering().cacheMode(CacheMode.LOCAL);
      cm.defineConfiguration("local-cache", cb.build());
   }

   @Override
   protected void createCacheManagers() throws Exception {
      Util.recursiveFileRemove(PERSISTENT_LOCATION);
      super.createCacheManagers();
   }

   @BeforeMethod(alwaysRun = true)
   @Override
   public void beforeMethod() {
      super.beforeMethod();
      restServerClient = client.server();
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      afterClass();
      super.clearContent();
      cacheManagers.clear();
   }

   public void testReadyAndLive() {
      try (RestResponse res = join(restServerClient.ready())) {
         assertThat(res.status()).isEqualTo(OK);
      }

      try (RestResponse res = join(restServerClient.live())) {
         assertThat(res.status()).isEqualTo(OK);
      }
   }

   public void testReplyDuringShutdown() {
      manager(0).getCache(TEST_CACHE_NAME).put("key", "value");
      manager(0).getCache("local-cache").put("key", "value");

      try (RestResponse res = join(restServerClient.ready())) {
         assertThat(res.status()).isEqualTo(OK);
      }

      // Force a cluster shutdown. Calling the REST endpoint doesn't perform the shutdown because of the mock services.
      Stream.of(managers())
            .map(ecm -> (DefaultCacheManager) ecm)
            .forEach(dcm -> {
               if (security) {
                  Security.doAs(TestingUtil.makeSubject(AuthorizationPermission.ADMIN.name()), dcm::shutdownAllCaches);
               } else {
                  dcm.shutdownAllCaches();
               }
            });

      // Assert the cache registry is stopped.
      assertThat(manager(0).getCache(TEST_CACHE_NAME).getStatus().allowInvocations()).isFalse();

      // The cluster should still reply OK for live. Otherwise, the pod might be stopped.
      try (RestResponse res = join(restServerClient.live())) {
         assertThat(res.status()).isEqualTo(OK);
      }

      // It should reply 503 for readiness, since the cache is stopped and can't handle requests anymore.
      try (RestResponse res = join(restServerClient.ready())) {
         assertThat(res.status()).isEqualTo(SERVICE_UNAVAILABLE);
      }
   }
}
