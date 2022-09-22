package org.infinispan.server.security.authorization;

import java.nio.file.Paths;

import javax.security.auth.Subject;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.security.VoidCallbackHandler;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.LdapServerListener;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.experimental.categories.Category;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.1
 **/

@Category(Security.class)
public class AuthorizationKerberosIT extends AbstractAuthorization {
   @ClassRule
   public static InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/AuthorizationKerberosTest.xml")
               .numServers(1)
               .property("java.security.krb5.conf", "${infinispan.server.config.path}/krb5.conf")
               .addListener(new LdapServerListener(true))
               .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private static String oldKrb5Conf;

   @BeforeClass
   public static void setKrb5Conf() {
      oldKrb5Conf = System.setProperty("java.security.krb5.conf", Paths.get("src/test/resources/configuration/krb5.conf").toString());
   }

   @AfterClass
   public static void restoreKrb5Conf() {
      if (oldKrb5Conf != null) {
         System.setProperty("java.security.krb5.conf", oldKrb5Conf);
      }
   }

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
      RestClientConfigurationBuilder restBuilder = new RestClientConfigurationBuilder();
      if (user != TestUser.ANONYMOUS) {
         Subject subject = Common.createSubject(user.getUser(), "INFINISPAN.ORG", user.getPassword().toCharArray());
         hotRodBuilder.security().authentication()
               .saslMechanism("GSSAPI")
               .serverName("datagrid")
               .realm("default")
               .callbackHandler(new VoidCallbackHandler())
               .clientSubject(subject);
         restBuilder.security().authentication()
               .mechanism("SPNEGO")
               .clientSubject(subject);
      }
      hotRodBuilders.put(user, hotRodBuilder);
      restBuilders.put(user, restBuilder);
   }

   protected String expectedServerPrincipalName(TestUser user) {
      return String.format("%s@INFINISPAN.ORG", user.getUser());
   }
}
