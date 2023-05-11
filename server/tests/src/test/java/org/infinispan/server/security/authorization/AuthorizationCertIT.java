package org.infinispan.server.security.authorization;

import static org.infinispan.server.test.core.AbstractInfinispanServerDriver.KEY_PASSWORD;

import java.net.InetSocketAddress;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.experimental.categories.Category;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.resource.DefaultClientResources;

/**
 * @author Ryan Emerson
 * @since 13.0
 */
@Category(Security.class)
public class AuthorizationCertIT extends AbstractAuthorization {

   @ClassRule
   public static InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/AuthorizationCertTest.xml")
               .runMode(ServerRunMode.CONTAINER)
               .mavenArtifacts(ClusteredIT.mavenArtifacts())
               .artifacts(ClusteredIT.artifacts())
               .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Override
   protected InfinispanServerRule getServers() {
      return SERVERS;
   }

   @Override
   protected InfinispanServerTestMethodRule getServerTest() {
      return SERVER_TEST;
   }

   @Override
   protected void addClientBuilders(TestUser user) {
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

      hotRodBuilders.put(user, hotRodBuilder);

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
      restBuilders.put(user, restBuilder);

      addRespBuilder(user);
   }

   private void addRespBuilder(TestUser user) {
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

      respBuilders.put(user, new LettuceConfiguration(builder.build(), options, uri));
   }

   protected String expectedServerPrincipalName(TestUser user) {
      return String.format("CN=%s,OU=Infinispan,O=JBoss,L=Red Hat", user.getUser());
   }

   @Override
   protected boolean isUsingCert() {
      return true;
   }
}
