package org.infinispan.server.security.authorization;

import static org.infinispan.server.test.core.AbstractInfinispanServerDriver.KEY_PASSWORD;

import java.net.InetSocketAddress;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.RespTestClientDriver;
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

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.resource.DefaultClientResources;

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
            DefaultClientResources.Builder builder = DefaultClientResources.builder();
            SslOptions.Builder sslBuilder = SslOptions.builder()
                  .jdkSslProvider()
                  .truststore(SERVERS.getServerDriver().getCertificateFile("ca.pfx"), KEY_PASSWORD);

            RedisURI uri;
            InetSocketAddress serverSocket = SERVERS.getServerDriver().getServerSocket(0, 11222);
            if (user == TestUser.ANONYMOUS) {
               sslBuilder.keystore(SERVERS.getServerDriver().getCertificateFile("server.pfx"), KEY_PASSWORD.toCharArray())
                     .keyStoreType("pkcs12");
               uri = RedisURI.create(serverSocket.getHostString(), serverSocket.getPort());
            } else {
               sslBuilder.keystore(SERVERS.getServerDriver().getCertificateFile(user.getUser() + ".pfx"), KEY_PASSWORD.toCharArray())
                     .keyStoreType("pkcs12");
               uri = RedisURI.builder()
                     .withSsl(true)
                     .withHost(serverSocket.getHostString())
                     .withPort(serverSocket.getPort())
                     .withVerifyPeer(SslVerifyMode.NONE)
                     .build();
            }

            ClientOptions options = ClientOptions.builder()
                  .sslOptions(sslBuilder.build())
                  .autoReconnect(false) // Otherwise, Lettuce keeps retrying.
                  .build();

            return new RespTestClientDriver.LettuceConfiguration(builder, options, uri);
         });
      }
   }

   private static String expectedServerPrincipalName(TestUser user) {
      return String.format("CN=%s,OU=Infinispan,O=JBoss,L=Red Hat", user.getUser());
   }
}
