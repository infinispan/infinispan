package org.infinispan.server.security.authorization;

import static org.infinispan.server.test.core.AbstractInfinispanServerDriver.KEY_PASSWORD;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.server.test.junit5.InfinispanSuite;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import io.vertx.core.net.JdkSSLEngineOptions;
import io.vertx.core.net.PfxOptions;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisOptions;

/**
 * @author Ryan Emerson
 * @since 13.0
 */
@Suite
@SelectClasses({AuthorizationCertIT.HotRod.class, AuthorizationCertIT.Resp.class, AuthorizationCertIT.Rest.class})
@Category(Security.class)
public class AuthorizationCertIT extends InfinispanSuite {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthorizationCertTest.xml")
               .runMode(ServerRunMode.CONTAINER)
               .mavenArtifacts(ClusteredIT.mavenArtifacts())
               .artifacts(ClusteredIT.artifacts())
               .build();

   static class HotRod extends HotRodAuthorizationTest {
      @RegisterExtension
      static InfinispanServerExtension SERVERS = AuthorizationCertIT.SERVERS;

      public HotRod() {
         super(SERVERS, AuthorizationCertIT::expectedServerPrincipalName, user -> {
            ConfigurationBuilder hotRodBuilder = new ConfigurationBuilder();
            SERVERS.getServerDriver().applyTrustStore(hotRodBuilder, "ca.pfx");
            if (user == TestUser.ANONYMOUS) {
               SERVERS.getServerDriver().applyKeyStore(hotRodBuilder, "server.pfx");
            } else {
               SERVERS.getServerDriver().applyKeyStore(hotRodBuilder, user.getUser() + ".pfx");
            }
            hotRodBuilder.security()
                  .ssl().sniHostName("infinispan.test")
                  .authentication()
                  .saslMechanism("EXTERNAL")
                  .serverName("infinispan")
                  .realm("default");
            return hotRodBuilder;
         });
      }
   }

   static class Rest extends RESTAuthorizationTest {
      @RegisterExtension
      static InfinispanServerExtension SERVERS = AuthorizationCertIT.SERVERS;

      public Rest() {
         super(SERVERS, AuthorizationCertIT::expectedServerPrincipalName, user -> {
            RestClientConfigurationBuilder restBuilder = new RestClientConfigurationBuilder();
            SERVERS.getServerDriver().applyTrustStore(restBuilder, "ca.pfx");
            if (user == TestUser.ANONYMOUS) {
               SERVERS.getServerDriver().applyKeyStore(restBuilder, "server.pfx");
            } else {
               SERVERS.getServerDriver().applyKeyStore(restBuilder, user.getUser() + ".pfx");
            }
            restBuilder.security().authentication().ssl()
                  .sniHostName("infinispan")
                  .hostnameVerifier((hostname, session) -> true).connectionTimeout(120_000).socketTimeout(120_000);
            return restBuilder;
         });
      }
   }

   static class Resp extends RESPAuthorizationTest {
      @RegisterExtension
      static InfinispanServerExtension SERVERS = AuthorizationCertIT.SERVERS;

      public Resp() {
         super(SERVERS, true, AuthorizationCertIT::expectedServerPrincipalName, user -> {
            int size = SERVERS.getServerDriver().getConfiguration().numServers();
            RedisOptions options = new RedisOptions()
                  .setPoolName("pool-" + user.getUser());

            if (size > 1) {
               options = options.setType(RedisClientType.CLUSTER);
            } else {
               options = options.setType(RedisClientType.STANDALONE);
            }

            for (int i = 0; i < size; i++) {
               String uri = redisURI(SERVERS.getServerDriver().getServerSocket(i, 11222), null, user != TestUser.ANONYMOUS);
               options = options.addConnectionString(uri);
            }

            PfxOptions certOpts;
            if (user == TestUser.ANONYMOUS) {
               certOpts = new PfxOptions()
                     .setPath(SERVERS.getServerDriver().getCertificateFile("server.pfx").getPath())
                     .setPassword(KEY_PASSWORD);
            } else {
               certOpts = new PfxOptions()
                     .setPath(SERVERS.getServerDriver().getCertificateFile(user.getUser() + ".pfx").getPath())
                     .setPassword(KEY_PASSWORD);
            }

            PfxOptions trustOpts = new PfxOptions()
                  .setPath(SERVERS.getServerDriver().getCertificateFile("ca.pfx").getPath())
                  .setPassword(KEY_PASSWORD);
            options.getNetClientOptions()
                  .setSsl(true)
                  .setSslEngineOptions(new JdkSSLEngineOptions())
                  .setPfxKeyCertOptions(certOpts)
                  .setPfxTrustOptions(trustOpts);

            return options;
         });
      }
   }

   private static String expectedServerPrincipalName(TestUser user) {
      return String.format("CN=%s,OU=Infinispan,O=JBoss,L=Red Hat", user.getUser());
   }
}
