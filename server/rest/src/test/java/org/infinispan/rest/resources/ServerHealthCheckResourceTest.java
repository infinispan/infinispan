package org.infinispan.rest.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.rest.RestResponseInfo.OK;
import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.infinispan.partitionhandling.PartitionHandling.DENY_READ_WRITES;

import java.nio.file.Paths;
import java.util.stream.Stream;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestServerClient;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.ServerHealthCheckResourceTest")
public class ServerHealthCheckResourceTest extends AbstractRestResourceTest {

   private static final String PERSISTENT_LOCATION = tmpDirectory(ServerHealthCheckResourceTest.class.getName());
   private static final String TEST_CACHE_NAME = "testing-cache";

   private RestServerClient restServerClient;

   @Override
   public Object[] factory() {
      return Stream.of(Protocol.values())
            .flatMap(protocol ->
                  Stream.of(true, false)
                        .flatMap(security -> Stream.of(
                              new ServerHealthCheckResourceTest().withSecurity(security).protocol(protocol).browser(true),
                              new ServerHealthCheckResourceTest().withSecurity(security).protocol(protocol).browser(false)
                        )))
            .toArray(Object[]::new);
   }

   @Override
   protected GlobalConfigurationBuilder getGlobalConfigForNode(int id) {
      GlobalConfigurationBuilder config = super.getGlobalConfigForNode(id);
      config.globalState().enable()
            .configurationStorage(ConfigurationStorage.OVERLAY)
            .persistentLocation(Paths.get(PERSISTENT_LOCATION, Integer.toString(id)).toString())
            .metrics().accurateSize(true);
      return config;
   }

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.statistics().enable().clustering().cacheMode(DIST_SYNC).partitionHandling().whenSplit(DENY_READ_WRITES).aliases("alias");
      cm.defineConfiguration(TEST_CACHE_NAME, builder.build());
   }

   @Override
   protected void createCacheManagers() throws Exception {
      Util.recursiveFileRemove(PERSISTENT_LOCATION);
      super.createCacheManagers();
      restServerClient = client.server();
   }

   public void testReadyAndLive() {
      try (RestResponse res = join(restServerClient.ready())) {
         assertThat(res.status()).isEqualTo(OK);
      }

      try (RestResponse res = join(restServerClient.live())) {
         assertThat(res.status()).isEqualTo(OK);
      }
   }

   public void testReadyDuringShutdown() {
      manager(0).getCache(TEST_CACHE_NAME).put("key", "value");

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

      // The cluster should still reply OK for ready and live.
      try (RestResponse res = join(restServerClient.ready())) {
         assertThat(res.status()).isEqualTo(OK);
      }

      try (RestResponse res = join(restServerClient.live())) {
         assertThat(res.status()).isEqualTo(OK);
      }
   }
}
