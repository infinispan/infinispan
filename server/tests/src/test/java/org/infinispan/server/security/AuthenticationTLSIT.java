package org.infinispan.server.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.security.BasicCallbackHandler;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.tags.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.internal.OperationFuture;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/

@Security
public class AuthenticationTLSIT {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthenticationServerTLSTest.xml")
                                    .build();

   @ParameterizedTest
   @ArgumentsSource(Common.SaslMechsArgumentProvider.class)
   public void testHotRodReadWrite(String mechanism) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      builder.security().ssl().sniHostName("infinispan.test");
      if (!mechanism.isEmpty()) {
         builder.security().authentication()
               .saslMechanism(mechanism)
               .serverName("infinispan")
               .realm("default")
               .username("all_user")
               .password("all");
      }

      try {
         RemoteCache<String, String> cache = SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();
         cache.put("k1", "v1");
         assertEquals(1, cache.size());
         assertEquals("v1", cache.get("k1"));
      } catch (HotRodClientException e) {
         // Rethrow if unexpected
         if (!mechanism.isEmpty()) throw e;
      }
   }

   @ParameterizedTest
   @ArgumentsSource(Common.SaslMechsArgumentProvider.class)
   public void testMemcachedReadWrite(String mechanism) throws ExecutionException, InterruptedException, TimeoutException {
      ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();
      builder.setProtocol(mechanism.isEmpty() ? ConnectionFactoryBuilder.Protocol.TEXT : ConnectionFactoryBuilder.Protocol.BINARY);
      builder.setAuthDescriptor(new AuthDescriptor(new String[]{mechanism}, new BasicCallbackHandler("all_user", "default", "all".toCharArray())));
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      MemcachedClient client = SERVERS.memcached().withClientConfiguration(builder).get();
      OperationFuture<Boolean> f = client.set("k" + mechanism, 0, "v");
      assertTrue(f.get(10, TimeUnit.SECONDS));
      assertEquals(client.get("k" + mechanism), "v");
   }
}
