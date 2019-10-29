package org.infinispan.server.security;

import static org.infinispan.server.security.Common.HTTP_KERBEROS_MECHS;
import static org.infinispan.server.security.Common.HTTP_PROTOCOLS;
import static org.infinispan.server.security.Common.sync;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerRuleConfigurationBuilder;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.infinispan.server.test.LdapServerRule;
import org.infinispan.server.test.category.Security;
import org.infinispan.test.Exceptions;
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
public class AuthenticationKerberosSpnegoIT {
   @ClassRule
   public static InfinispanServerRule SERVERS = new InfinispanServerRule(new InfinispanServerRuleConfigurationBuilder("configuration/AuthenticationKerberosTest.xml")
         .numServers(1).property("java.security.krb5.conf", "${infinispan.server.config.path}/krb5.conf"));

   @ClassRule
   public static LdapServerRule LDAP = new LdapServerRule(SERVERS, "ldif/infinispan-kerberos.ldif", true);

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private final Protocol protocol;
   private final String mechanism;

   private static String oldKrb5Conf;

   @Parameterized.Parameters(name = "{1}({0})")
   public static Collection<Object[]> data() {
      List<Object[]> params = new ArrayList<>(HTTP_KERBEROS_MECHS.size() * HTTP_PROTOCOLS.size());
      for (Protocol protocol : HTTP_PROTOCOLS) {
         for (Object[] mech : HTTP_KERBEROS_MECHS) {
            params.add(new Object[]{protocol, mech[0]});
         }
      }
      return params;
   }

   public AuthenticationKerberosSpnegoIT(Protocol protocol, String mechanism) {
      this.protocol = protocol;
      this.mechanism = mechanism;
   }

   @BeforeClass
   public static void setKrb5Conf() {
      oldKrb5Conf = System.setProperty("java.security.krb5.conf", AuthenticationKerberosSpnegoIT.class.getClassLoader().getResource("configuration/krb5.conf").getPath());
   }

   @AfterClass
   public static void restoreKrb5Conf() {
      if (oldKrb5Conf != null) {
         System.setProperty("java.security.krb5.conf", oldKrb5Conf);
      }
   }

   @Test
   public void testReadWrite() {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      if (!mechanism.isEmpty()) {
         builder
               .protocol(protocol)
               .security().authentication()
               .mechanism(mechanism)
               .clientSubject(Common.createSubject("admin", "INFINISPAN.ORG", "strongPassword".toCharArray()));
      }
      if (mechanism.isEmpty()) {
         Exceptions.expectException(RuntimeException.class, () -> SERVER_TEST.rest().withClientConfiguration(builder).create());
      } else {
         RestClient client = SERVER_TEST.rest().withClientConfiguration(builder).create();
         RestResponse response = sync(client.cache(SERVER_TEST.getMethodName()).post("k1", "v1"));
         assertEquals(204, response.getStatus());
         assertEquals(protocol, response.getProtocol());
         response = sync(client.cache(SERVER_TEST.getMethodName()).get("k1"));
         assertEquals(200, response.getStatus());
         assertEquals(protocol, response.getProtocol());
         assertEquals("v1", response.getBody());
      }
   }
}
