package org.infinispan.server.security.authorization;

import static org.infinispan.server.test.core.TestSystemPropertyNames.HOTROD_CLIENT_SASL_MECHANISM;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.server.test.api.RespTestClientDriver;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.resource.DefaultClientResources;


/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/

public abstract class AbstractAuthorization {

   private boolean initialized = false;
   final Map<TestUser, RespTestClientDriver.LettuceConfiguration> respBuilders = new HashMap<>();
   final Map<TestUser, ConfigurationBuilder> hotRodBuilders = new HashMap<>();
   final Map<TestUser, RestClientConfigurationBuilder> restBuilders = new HashMap<>();
   final Map<String, String> bulkData = new HashMap<>();

   protected abstract InfinispanServerRule getServers();

   protected abstract InfinispanServerTestMethodRule getServerTest();

   protected void addClientBuilders(TestUser user) {
      InetSocketAddress serverSocket = getServers().getServerDriver().getServerSocket(0, 11222);
      ClientOptions clientOptions = ClientOptions.builder()
            .autoReconnect(false)
            .build();
      RedisURI.Builder uriBuilder = RedisURI.builder()
            .withHost(serverSocket.getHostString())
            .withPort(serverSocket.getPort());

      if (user != TestUser.ANONYMOUS) {
         uriBuilder = uriBuilder.withAuthentication(user.getUser(), user.getPassword());
      }
      respBuilders.put(user, new RespTestClientDriver.LettuceConfiguration(DefaultClientResources.builder(), clientOptions, uriBuilder.build()));

      ConfigurationBuilder hotRodBuilder = new ConfigurationBuilder();
      hotRodBuilder.security().authentication()
            .saslMechanism(System.getProperty(HOTROD_CLIENT_SASL_MECHANISM, "SCRAM-SHA-1"))
            .serverName("infinispan")
            .realm("default")
            .username(user.getUser())
            .password(user.getPassword());
      hotRodBuilders.put(user, hotRodBuilder);

      RestClientConfigurationBuilder restBuilder = new RestClientConfigurationBuilder();
      restBuilder.security().authentication()
            .mechanism("AUTO")
            .username(user.getUser())
            .password(user.getPassword());
      restBuilders.put(user, restBuilder);
   }

   protected String expectedServerPrincipalName(TestUser user) {
      return user.getUser();
   }

   public final synchronized void init() {
      if (initialized) return;
      initialized = true;

      for (int i = 0; i < 10; i++) {
         bulkData.put("k" + i, "v" + i);
      }

      Stream.of(TestUser.values()).forEach(this::addClientBuilders);
   }

   protected boolean isUsingCert() {
      return false;
   }
}
