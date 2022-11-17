package org.infinispan.server.security;

import static org.infinispan.server.test.core.Common.HTTP_KERBEROS_MECHS;
import static org.infinispan.server.test.core.Common.HTTP_PROTOCOLS;
import static org.infinispan.server.test.core.Common.sync;
import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.security.VoidCallbackHandler;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
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
   public static InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/AuthenticationKerberosTest.xml")
                                    .numServers(1)
                                    .property("java.security.krb5.conf", "${infinispan.server.config.path}/krb5.conf")
                                    .addListener(new LdapServerListener(true))
                                    .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private final String protocol;
   private final String mechanism;

   private static String oldKrb5Conf;

   @Parameterized.Parameters(name = "{1}({0})")
   public static Collection<Object[]> data() {
      List<Object[]> params = new ArrayList<>();
      for(Object[] mech : Common.SASL_KERBEROS_MECHS) {
         params.add(new Object[]{"Hot Rod", mech[0]});
      }
      for (Protocol protocol : HTTP_PROTOCOLS) {
         for (Object[] mech : HTTP_KERBEROS_MECHS) {
            params.add(new Object[]{protocol.name(), mech[0]});
         }
      }
      return params;
   }

   public AuthenticationKerberosIT(String protocol, String mechanism) {
      this.protocol = protocol;
      this.mechanism = mechanism;
   }

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

   @Test
   public void testProtocol() {
      if ("Hot Rod".equals(protocol)) {
         testHotRod();
      } else {
         testRest(Protocol.valueOf(protocol));
      }
   }

   public void testHotRod() {
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

   public void testRest(Protocol protocol) {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      if (!mechanism.isEmpty()) {
         builder
               .protocol(protocol)
               .security().authentication()
               .mechanism(mechanism)
               .clientSubject(Common.createSubject("admin", "INFINISPAN.ORG", "strongPassword".toCharArray()));
         // Kerberos is strict about the hostname, so we do this by hand
         InetSocketAddress serverAddress = SERVERS.getServerDriver().getServerSocket(0, 11222);
         builder.addServer().host(serverAddress.getHostName()).port(serverAddress.getPort());
      }
      if (mechanism.isEmpty()) {
         Exceptions.expectException(SecurityException.class, () -> SERVER_TEST.rest().withClientConfiguration(builder).create());
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
