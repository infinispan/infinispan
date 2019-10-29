package org.infinispan.server.security;

import static org.infinispan.server.security.Common.sync;
import static org.junit.Assert.assertEquals;

import javax.security.auth.callback.Callback;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerRuleConfigurationBuilder;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.infinispan.server.test.KeyCloakServerRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.wildfly.security.auth.callback.CredentialCallback;
import org.wildfly.security.credential.BearerTokenCredential;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class AuthenticationKeyCloakIT {

   @ClassRule
   public static InfinispanServerRule SERVERS = new InfinispanServerRule(new InfinispanServerRuleConfigurationBuilder("configuration/AuthenticationKeyCloakTest.xml"));

   @ClassRule
   public static KeyCloakServerRule KEYCLOAK = new KeyCloakServerRule("keycloak/infinispan-keycloak-realm.json");

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testHotRodReadWrite() {
      String token = KEYCLOAK.getAccessTokenForCredentials("infinispan", "infinispan-client", "8a43581d-62d7-47dc-9aa4-cd3af24b6083", "admin", "adminPassword");

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.security().authentication()
            .saslMechanism("OAUTHBEARER")
            .serverName("infinispan")
            .callbackHandler(callbacks -> {
               for (Callback callback : callbacks) {
                  if (callback instanceof CredentialCallback) {
                     CredentialCallback cc = (CredentialCallback) callback;
                     cc.setCredential(new BearerTokenCredential(token));
                  }
               }
            })
            .realm("default");

      RemoteCache<String, String> cache = SERVER_TEST.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();
      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
   }

   @Test
   public void testRestReadWrite() {
      String token = KEYCLOAK.getAccessTokenForCredentials("infinispan", "infinispan-client", "8a43581d-62d7-47dc-9aa4-cd3af24b6083", "admin", "adminPassword");

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
