package org.infinispan.server.security;

import static org.infinispan.server.security.Common.sync;
import static org.junit.Assert.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.infinispan.server.test.core.KeyCloakServerRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class AuthenticationKeyCloakIT {

   @ClassRule
   public static final InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/AuthenticationKeyCloakTest.xml")
                                    .build();

   public static final String INFINISPAN_REALM = "infinispan";
   public static final String INFINISPAN_CLIENT_ID = "infinispan-client";
   public static final String INFINISPAN_CLIENT_SECRET = "8a43581d-62d7-47dc-9aa4-cd3af24b6083";


   @ClassRule
   public static KeyCloakServerRule KEYCLOAK = new KeyCloakServerRule(System.getProperty(TestSystemPropertyNames.KEYCLOAK_REALM, "keycloak/infinispan-keycloak-realm.json"));

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testHotRodReadWrite() {
      String token = KEYCLOAK.getAccessTokenForCredentials(INFINISPAN_REALM, INFINISPAN_CLIENT_ID, INFINISPAN_CLIENT_SECRET, "admin", "adminPassword");

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.security().authentication()
            .saslMechanism("OAUTHBEARER")
            .serverName(INFINISPAN_REALM)
            .realm("default")
            .token(token);

      RemoteCache<String, String> cache = SERVER_TEST.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();
      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
   }

   @Test
   public void testRestReadWrite() {
      String token = KEYCLOAK.getAccessTokenForCredentials(INFINISPAN_REALM, INFINISPAN_CLIENT_ID, INFINISPAN_CLIENT_SECRET, "admin", "adminPassword");

      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.security().authentication()
            .mechanism("BEARER_TOKEN")
            .username(token);
      RestClient client = SERVER_TEST.rest().withClientConfiguration(builder).create();
      RestResponse response = sync(client.cache(SERVER_TEST.getMethodName()).post("k1", "v1"));

      assertEquals(204, response.getStatus());
      response = sync(client.cache(SERVER_TEST.getMethodName()).get("k1"));
      assertEquals(200, response.getStatus());
      assertEquals("v1", response.getBody());
   }
}
