package org.infinispan.server.security.authorization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.rest.RestResponseInfo.NO_CONTENT;
import static org.infinispan.server.test.core.Common.sync;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.client.rest.configuration.RestClientConfigurationProperties;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.file.SingleFileStoreConfigurationBuilder;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.artifacts.Artifacts;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.testing.Eventually;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class AuthorizationGracefulShutdownIT {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthorizationPropertiesTest.xml")
               .runMode(ServerRunMode.CONTAINER)
               .mavenArtifacts(ClusteredIT.mavenArtifacts())
               .artifacts(Artifacts.artifacts())
               .numServers(2)
               .property(INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED, "true")
               .build();

   @Test
   public void testGracefulShutdownRestart() throws Exception {
      TestUser user = TestUser.ADMIN;
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).persistence().addStore(SingleFileStoreConfigurationBuilder.class).segmented(false);
      RemoteCache<Object, Object> hotRod = SERVERS.hotrod().withServerConfiguration(builder).withClientConfiguration(clientHotRodSecurityConfiguration(user)).create();

      populateCache(hotRod);
      shutdownAndRestart(user);
      assertCacheData(hotRod);
   }

   private org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientHotRodSecurityConfiguration(TestUser user) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      builder.security().authentication()
            .saslMechanism("SCRAM-SHA-1")
            .serverName("infinispan")
            .realm("default")
            .username(user.getUser())
            .password(user.getPassword());

      return builder;
   }

   private void shutdownAndRestart(TestUser user) throws Exception {
      for (int i = 0; i < SERVERS.getServerDriver().getConfiguration().numServers(); i++) {
         RestClientConfigurationBuilder restClientBuilder = new RestClientConfigurationBuilder()
               .socketTimeout(RestClientConfigurationProperties.DEFAULT_SO_TIMEOUT * 60)
               .connectionTimeout(RestClientConfigurationProperties.DEFAULT_CONNECT_TIMEOUT * 60);
         restClientBuilder.security().authentication()
               .mechanism("AUTO")
               .username(user.getUser())
               .password(user.getPassword());
         RestClient rest = SERVERS.rest().withClientConfiguration(restClientBuilder).get(i);
         try (RestResponse res = sync(rest.container().shutdown(), 15, TimeUnit.SECONDS)) {
            if (i == 0) assertThat(res.status()).isEqualTo(NO_CONTENT);
            else assertThat(res.status()).isEqualTo(500); // TODO: Should return 503 after cluster-wide graceful shutdown.
         }
         rest.close();
      }
      SERVERS.getServerDriver().stopCluster();
      ContainerInfinispanServerDriver serverDriver = (ContainerInfinispanServerDriver) SERVERS.getServerDriver();
      Eventually.eventually(
            "Cluster did not shutdown within timeout",
            () -> (!serverDriver.isRunning(0) && !serverDriver.isRunning(1)),
            serverDriver.getTimeout(), 1, TimeUnit.SECONDS);

      serverDriver.restartCluster();
   }

   private void populateCache(RemoteCache<Object, Object> hotRod) {
      for (int i = 0; i < 100; i++) {
         hotRod.put(String.format("k%03d", i), String.format("v%03d", i));
      }
   }

   private void assertCacheData(RemoteCache<Object, Object> hotRod) {
      for (int i = 0; i < 100; i++) {
         assertThat(hotRod.get(String.format("k%03d", i)))
               .isEqualTo(String.format("v%03d", i));
      }
   }
}
