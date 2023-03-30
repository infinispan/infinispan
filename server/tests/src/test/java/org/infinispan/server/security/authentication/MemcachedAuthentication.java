package org.infinispan.server.security.authentication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.security.Provider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.security.BasicCallbackHandler;
import org.infinispan.commons.util.Util;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.internal.OperationFuture;

/**
 * @since 15.0
 **/

@RunWith(Parameterized.class)
@Category(Security.class)
public class MemcachedAuthentication {

   @ClassRule
   public static InfinispanServerRule SERVERS = AuthenticationIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private final String mechanism;

   private static final Provider[] SECURITY_PROVIDERS;

   static {
      // Register only the providers that matter to us
      List<Provider> providers = new ArrayList<>();
      for (String name : Arrays.asList(
            "org.wildfly.security.sasl.plain.WildFlyElytronSaslPlainProvider",
            "org.wildfly.security.sasl.digest.WildFlyElytronSaslDigestProvider",
            "org.wildfly.security.sasl.external.WildFlyElytronSaslExternalProvider",
            "org.wildfly.security.sasl.oauth2.WildFlyElytronSaslOAuth2Provider",
            "org.wildfly.security.sasl.scram.WildFlyElytronSaslScramProvider",
            "org.wildfly.security.sasl.gssapi.WildFlyElytronSaslGssapiProvider",
            "org.wildfly.security.sasl.gs2.WildFlyElytronSaslGs2Provider"
      )) {
         Provider provider = Util.getInstance(name, RemoteCacheManager.class.getClassLoader());
         providers.add(provider);
      }
      SECURITY_PROVIDERS = providers.toArray(new Provider[0]);
   }

   @Parameterized.Parameters(name = "{0}")
   public static Collection<Object[]> data() {
      return Common.SASL_MECHS;
   }

   public MemcachedAuthentication(String mechanism) {
      this.mechanism = mechanism;
   }

   @Test
   public void testMemcachedReadWrite() throws ExecutionException, InterruptedException, TimeoutException {
      ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();
      builder.setProtocol(mechanism.isEmpty() ? ConnectionFactoryBuilder.Protocol.TEXT : ConnectionFactoryBuilder.Protocol.BINARY);
      builder.setAuthDescriptor(new AuthDescriptor(new String[]{mechanism}, new BasicCallbackHandler("all_user", "default", "all".toCharArray()), null, null, SECURITY_PROVIDERS));
      MemcachedClient client = SERVER_TEST.memcached().withClientConfiguration(builder).get();
      OperationFuture<Boolean> f = client.set("k" + mechanism, 0, "v");
      assertTrue(f.get(10, TimeUnit.SECONDS));
      assertEquals(client.get("k" + mechanism), "v");
   }
}
