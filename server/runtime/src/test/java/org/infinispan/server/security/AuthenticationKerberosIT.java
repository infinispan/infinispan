package org.infinispan.server.security;

import static org.junit.Assert.assertEquals;

import java.util.Collection;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.security.VoidCallbackHandler;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerRuleConfigurationBuilder;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.infinispan.server.test.LdapServerRule;
import org.infinispan.server.test.category.Security;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.1
 **/

@RunWith(Parameterized.class)
@Category(Security.class)
public class AuthenticationKerberosIT {
   @ClassRule
   public static InfinispanServerRule SERVERS = new InfinispanServerRule(new InfinispanServerRuleConfigurationBuilder("configuration/AuthenticationKerberosTest.xml")
         .numServers(1).property("java.security.krb5.conf", "${infinispan.server.config.path}/krb5.conf"));

   @ClassRule
   public static LdapServerRule LDAP = new LdapServerRule(SERVERS, "ldif/infinispan-kerberos.ldif", true);

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private final String mechanism;
   private static String oldKrb5Conf;

   @Parameterized.Parameters(name = "{0}")
   public static Collection<Object[]> data() {
      return Common.SASL_KERBEROS_MECHS;
   }

   public AuthenticationKerberosIT(String mechanism) {
      this.mechanism = mechanism;
   }

   @BeforeClass
   public static void setKrb5Conf() {
      oldKrb5Conf = System.setProperty("java.security.krb5.conf", AuthenticationKerberosIT.class.getClassLoader().getResource("configuration/krb5.conf").getPath());
   }

   @AfterClass
   public static void restoreKrb5Conf() {
      if (oldKrb5Conf != null) {
         System.setProperty("java.security.krb5.conf", oldKrb5Conf);
      }
   }

   @Test
   public void testReadWrite() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      if (!mechanism.isEmpty()) {
         builder.security().authentication()
               .saslMechanism(mechanism)
               .serverName("datagrid")
               .callbackHandler(new VoidCallbackHandler())
               .clientSubject(Common.createSubject("admin", "INFINISPAN.ORG", "strongPassword".toCharArray()));
      }

      try {
         RemoteCache<String, String> cache = SERVER_TEST.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();
         cache.put("k1", "v1");
         assertEquals(1, cache.size());
         assertEquals("v1", cache.get("k1"));
      } catch (HotRodClientException e) {
         // Rethrow if unexpected
         if (!mechanism.isEmpty()) throw e;
      }
   }
}
