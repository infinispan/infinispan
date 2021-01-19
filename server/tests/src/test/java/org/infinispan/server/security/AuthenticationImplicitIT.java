package org.infinispan.server.security;

import static org.infinispan.server.security.Common.HTTP_MECHS;
import static org.infinispan.server.security.Common.HTTP_PROTOCOLS;
import static org.infinispan.server.security.Common.sync;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.LdapServerRule;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/

@RunWith(Parameterized.class)
@Category(Security.class)
public class AuthenticationImplicitIT {
   @ClassRule
   public static InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/AuthenticationServerImplicitTest.xml")
                                    .addListener(new SecurityRealmServerListener("alternate"))
                                    .build();

   @ClassRule
   public static LdapServerRule LDAP = new LdapServerRule(SERVERS);

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private final String mechanism;
   private final String protocol;

   @Parameterized.Parameters(name = "{1}({0})")
   public static Collection<Object[]> data() {
      List<Object[]> params = new ArrayList<>();
      for(Object[] mech : Common.SASL_MECHS) {
         params.add(new Object[]{"Hot Rod", mech[0]});
      }
      for (Protocol protocol : HTTP_PROTOCOLS) {
         for (Object[] mech : HTTP_MECHS) {
            params.add(new Object[]{protocol.name(), mech[0]});
         }
      }
      return params;
   }

   public AuthenticationImplicitIT(String protocol, String mechanism) {
      this.protocol = protocol;
      this.mechanism = mechanism;
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
               .realm("default")
               .username("all_user")
               .password("all");
      }

      try {
         RemoteCache<String, String> cache = SERVER_TEST.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();
         cache.put("k1", "v1");
         assertEquals(1, cache.size());
         assertEquals("v1", cache.get("k1"));
      } catch (HotRodClientException e) {
         // Rethrow if unexpected
         if (!mechanism.isEmpty() && !"PLAIN".equals(mechanism)) throw e;
      }
   }

   public void testRest(Protocol protocol) {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      if (!mechanism.isEmpty()) {
         builder
               .protocol(protocol)
               .security().authentication()
               .mechanism(mechanism)
               .realm("default")
               .username("all_user")
               .password("all");
      }
      if (mechanism.isEmpty() || "BASIC".equals(mechanism)) {
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
