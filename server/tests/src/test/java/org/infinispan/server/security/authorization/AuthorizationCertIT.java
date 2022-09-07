package org.infinispan.server.security.authorization;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.experimental.categories.Category;

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
   }
}
