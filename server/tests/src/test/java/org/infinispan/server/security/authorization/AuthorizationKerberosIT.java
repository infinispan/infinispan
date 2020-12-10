package org.infinispan.server.security.authorization;

import javax.security.auth.Subject;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.security.VoidCallbackHandler;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.LdapServerRule;
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
                                    .build();

   @ClassRule
   public static LdapServerRule LDAP = new LdapServerRule(SERVERS, "ldif/infinispan-kerberos.ldif", true);

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private static String oldKrb5Conf;

   @BeforeClass
   public static void setKrb5Conf() {
      oldKrb5Conf = System.setProperty("java.security.krb5.conf",
            AuthorizationKerberosIT.class.getClassLoader().getResource("configuration/krb5.conf").getPath());
   }

   @AfterClass
   public static void restoreKrb5Conf() {
      if (oldKrb5Conf != null) {
         System.setProperty("java.security.krb5.conf", oldKrb5Conf);
      }
   }

   @Override
   protected InfinispanServerTestMethodRule getServerTest() {
      return SERVER_TEST;
   }

   @Override
   protected void addClientBuilders(String username, String password) {
      ConfigurationBuilder hotRodBuilder = new ConfigurationBuilder();
      Subject subject = Common.createSubject(username, "INFINISPAN.ORG", password.toCharArray());
      hotRodBuilder.security().authentication()
            .saslMechanism("GSSAPI")
            .serverName("datagrid")
            .realm("default")
            .callbackHandler(new VoidCallbackHandler())
            .clientSubject(subject);
      hotRodBuilders.put(username, hotRodBuilder);
      RestClientConfigurationBuilder restBuilder = new RestClientConfigurationBuilder();
      restBuilder.security().authentication()
            .mechanism("SPNEGO")
            .clientSubject(subject);
      restBuilders.put(username, restBuilder);
   }
}
