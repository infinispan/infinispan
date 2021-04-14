package org.infinispan.server.security;

import static org.infinispan.server.test.core.Common.sync;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.LdapServerListener;
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
import org.wildfly.security.http.HttpConstants;
import org.wildfly.security.sasl.util.SaslMechanismInformation;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/

@RunWith(Parameterized.class)
@Category(Security.class)
public class AuthenticationMultiEndpointIT {
   @ClassRule
   public static InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/AuthenticationServerMultipleEndpoints.xml")
               .addListener(new SecurityRealmServerListener("alternate"))
               .addListener(new LdapServerListener())
               .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private final String mechanism;
   private final String protocol;
   private final String realm;
   private final String userPrefix;
   private final int port;
   private final boolean isAnonymous;
   private final boolean isAdmin;
   private final boolean isPlain;
   private final boolean isAlternateRealm;
   private final boolean useAuth;
   private final boolean isMechanismClearText;

   @Parameterized.Parameters(name = "protocol={0}, mech={1}, realm={2}, userPrefix={3}, port={4}, anon={5}, admin={6}, plain={7}")
   public static Collection<Object[]> data() {
      List<Object[]> params = new ArrayList<>();

      // We test against different realms.
      for (String realm : Arrays.asList("default", "alternate")) {

         String userPrefix = "alternate".equals(realm) ? "alternate_" : "";
         // We test against different ports with different configurations
         for (int p = 11222; p < 11227; p++) {
            Integer port = Integer.valueOf(p);

            final boolean isAnonymous;
            final boolean isAdmin;
            final boolean isPlain;
            final boolean isAlternateRealmHotRod;
            final boolean isAlternateRealmHTTP;
            switch (p) {
               case 11222:
                  isAnonymous = false;
                  isAdmin = true;
                  isPlain = true;
                  isAlternateRealmHotRod = false;
                  isAlternateRealmHTTP = false;
                  break;
               case 11223:
                  isAnonymous = true;
                  isAdmin = false;
                  isPlain = false;
                  isAlternateRealmHotRod = false;
                  isAlternateRealmHTTP = false;
                  break;
               case 11224:
                  isAnonymous = false;
                  isAdmin = false;
                  isPlain = true;
                  isAlternateRealmHotRod = true;
                  isAlternateRealmHTTP = true;
                  break;
               case 11225:
                  isAnonymous = false;
                  isAdmin = true;
                  isPlain = false;
                  isAlternateRealmHotRod = true;
                  isAlternateRealmHTTP = false;
                  break;
               case 11226:
                  isAnonymous = false;
                  isAdmin = true;
                  isPlain = false;
                  isAlternateRealmHotRod = false;
                  isAlternateRealmHTTP = false;
                  break;
               default:
                  throw new IllegalArgumentException();
            }

            // We test with different Hot Rod mechs
            Common.SASL_MECHS.stream().map(m -> m[0]).forEach(m -> {
               params.add(new Object[]{"Hot Rod", m, realm, userPrefix, port, isAnonymous, isAdmin, isPlain, isAlternateRealmHotRod});
            });

            Common.HTTP_MECHS.stream().map(m -> m[0]).forEach(m -> {
               params.add(new Object[]{Protocol.HTTP_11.name(), m, realm, userPrefix, port, isAnonymous, isAdmin, isPlain, isAlternateRealmHTTP});
            });
         }
      }
      return params;
   }

   public AuthenticationMultiEndpointIT(String protocol, String mechanism, String realm, String userPrefix, int port, boolean isAnonymous, boolean isAdmin, boolean isPlain, boolean isAlternateRealm) {
      this.protocol = protocol;
      this.mechanism = mechanism;
      this.realm = realm;
      this.userPrefix = userPrefix;
      this.port = port;
      this.isAdmin = isAdmin;
      this.isAnonymous = isAnonymous;
      this.isPlain = isPlain;
      this.isAlternateRealm = isAlternateRealm;
      this.useAuth = !mechanism.isEmpty();
      this.isMechanismClearText = SaslMechanismInformation.Names.PLAIN.equals(mechanism) || HttpConstants.BASIC_NAME.equals(mechanism);
   }

   @Test
   public void testProtocol() {
      switch (protocol) {
         case "Hot Rod":
            testHotRod();
            break;
         default:
            testRest();
            break;
      }
   }

   private void testHotRod() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      if (useAuth) {
         builder.security().authentication()
               .saslMechanism(mechanism)
               .realm(realm)
               .username(userPrefix + "all_user")
               .password("all");
      }
      try {
         RemoteCache<String, String> cache = SERVER_TEST.hotrod().withClientConfiguration(builder).withPort(port).withCacheMode(CacheMode.DIST_SYNC).create();
         validateSuccess();
         cache.put("k1", "v1");
         assertEquals(1, cache.size());
         assertEquals("v1", cache.get("k1"));
      } catch (HotRodClientException e) {
         validateException(e);
      }
   }

   private void testRest() {
      Protocol proto = Protocol.valueOf(protocol);
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder().followRedirects(false);
      if (useAuth) {
         builder
               .protocol(proto)
               .security().authentication()
               .mechanism(mechanism)
               .realm(realm)
               .username(userPrefix + "all_user")
               .password("all");
      }

      try {
         RestClient client = SERVER_TEST.rest().withClientConfiguration(builder).withPort(port).create();
         validateSuccess();
         RestResponse response = sync(client.cache(SERVER_TEST.getMethodName()).post("k1", "v1"));
         assertEquals(204, response.getStatus());
         assertEquals(proto, response.getProtocol());
         response = sync(client.cache(SERVER_TEST.getMethodName()).get("k1"));
         assertEquals(200, response.getStatus());
         assertEquals(proto, response.getProtocol());
         assertEquals("v1", response.getBody());

         response = sync(client.raw().get("/"));
         assertEquals(isAdmin ? 307 : 404, response.getStatus());
         response = sync(client.server().info());
         assertEquals(isAdmin ? 200 : 404, response.getStatus());
      } catch (SecurityException e) {
         validateException(e);
      }
   }

   private void validateSuccess() {
      if (isAnonymous && useAuth) {
         throw new IllegalStateException("Authenticated client should not be allowed to connect to anonymous server");
      }
      if (!isAnonymous && !useAuth) {
         throw new IllegalStateException("Unauthenticated client should not be allowed to connect to authenticated server");
      }
   }

   private void validateException(RuntimeException e) {
      if (useAuth && isAnonymous) return;
      if (!useAuth && !isAnonymous) return;
      if (isAlternateRealm && "default".equals(realm)) return;
      if (!isAlternateRealm && !"default".equals(realm)) return;
      if (isPlain && !isMechanismClearText) return;
      if (!isPlain && isMechanismClearText) return;
      throw e;
   }
}
