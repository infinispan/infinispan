package org.infinispan.server.security;

import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.assertStatusAndBodyEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public abstract class AbstractAuthenticationKeyCloak {
   public static final String INFINISPAN_REALM = "infinispan";
   public static final String INFINISPAN_CLIENT_ID = "infinispan-client";
   public static final String INFINISPAN_CLIENT_SECRET = "8a43581d-62d7-47dc-9aa4-cd3af24b6083";

   protected final InfinispanServerExtension ext;

   public AbstractAuthenticationKeyCloak(InfinispanServerExtension ext) {
      this.ext = ext;
   }

   @Test
   public void testHotRodReadWrite() {
      String token = getToken();

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.security().authentication()
            .saslMechanism("OAUTHBEARER")
            .serverName(INFINISPAN_REALM)
            .realm("default")
            .token(token);

      RemoteCache<String, String> cache = ext.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();
      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
   }

   @Test
   public void testRestReadWrite() {
      String token = getToken();

      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.security().authentication()
            .mechanism("BEARER_TOKEN")
            .username(token);
      RestClient client = ext.rest().withClientConfiguration(builder).create();
      assertStatus(204, client.cache(ext.getMethodName()).post("k1", "v1"));
      assertStatusAndBodyEquals(200, "v1", client.cache(ext.getMethodName()).get("k1"));
   }

   protected abstract String getToken();
}
