package org.infinispan.server.security;

import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.sync;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.client.rest.configuration.RestClientConfigurationProperties;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.aggregator.AggregateWith;
import org.junit.jupiter.params.aggregator.ArgumentsAccessor;
import org.junit.jupiter.params.aggregator.ArgumentsAggregationException;
import org.junit.jupiter.params.aggregator.ArgumentsAggregator;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.wildfly.security.http.HttpConstants;
import org.wildfly.security.sasl.util.SaslMechanismInformation;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/

@Category(Security.class)
@Tag("embedded")
public class AuthenticationMultiEndpointIT {
   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthenticationServerMultipleEndpoints.xml")
               .addListener(new SecurityRealmServerListener("alternate")).numServers(1)
               .build();

   static class ArgsProvider implements ArgumentsProvider {
      @Override
      public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
         List<Arguments> args = new ArrayList<>();
         // We test against different realms.
         for (String realm : Arrays.asList("default", "alternate")) {

            String userPrefix = "alternate".equals(realm) ? "alternate_" : "";
            // We test against different ports with different configurations
            for (int p = 11222; p < 11227; p++) {
               final boolean isAnonymous;
               final boolean isAdmin;
               final boolean isPlain;
               final boolean isAlternateRealmHotRod;
               final boolean isAlternateRealmHTTP;
               final String contextPath;
               switch (p) {
                  case 11222:
                     isAnonymous = false;
                     isAdmin = true;
                     isPlain = true;
                     isAlternateRealmHotRod = false;
                     isAlternateRealmHTTP = false;
                     contextPath = RestClientConfigurationProperties.DEFAULT_CONTEXT_PATH;
                     break;
                  case 11223:
                     isAnonymous = true;
                     isAdmin = false;
                     isPlain = false;
                     isAlternateRealmHotRod = false;
                     isAlternateRealmHTTP = false;
                     contextPath = RestClientConfigurationProperties.DEFAULT_CONTEXT_PATH;
                     break;
                  case 11224:
                     isAnonymous = false;
                     isAdmin = false;
                     isPlain = true;
                     isAlternateRealmHotRod = true;
                     isAlternateRealmHTTP = true;
                     contextPath = RestClientConfigurationProperties.DEFAULT_CONTEXT_PATH;
                     break;
                  case 11225:
                     isAnonymous = false;
                     isAdmin = true;
                     isPlain = false;
                     isAlternateRealmHotRod = true;
                     isAlternateRealmHTTP = false;
                     contextPath = "/relax";
                     break;
                  case 11226:
                     isAnonymous = false;
                     isAdmin = true;
                     isPlain = false;
                     isAlternateRealmHotRod = false;
                     isAlternateRealmHTTP = false;
                     contextPath = RestClientConfigurationProperties.DEFAULT_CONTEXT_PATH;
                     break;
                  default:
                     throw new IllegalArgumentException();
               }

               // We test with different Hot Rod mechs
               int port = p;
               Common.SASL_MECHS.forEach(m ->
                     args.add(Arguments.of("Hot Rod", m, realm, userPrefix, port, isAnonymous, isAdmin, isPlain, isAlternateRealmHotRod, null))
               );

               Common.HTTP_MECHS.forEach(m ->
                     args.add(Arguments.of(Protocol.HTTP_11.name(), m, realm, userPrefix, port, isAnonymous, isAdmin, isPlain, isAlternateRealmHTTP, contextPath))
               );
            }
         }
         return args.stream();
      }
   }

   @ParameterizedTest(name = "protocol={0}, mech={1}, realm={2}, userPrefix={3}, port={4}, anon={5}, admin={6}, plain={7}, contextPath={9}")
   @ArgumentsSource(AuthenticationMultiEndpointIT.ArgsProvider.class)
   public void testProtocol(@AggregateWith(EndpointAggregator.class) Endpoint endpoint) {
      endpoint.test();
   }

   static class EndpointAggregator implements ArgumentsAggregator {
      @Override
      public Object aggregateArguments(ArgumentsAccessor accessor, ParameterContext context) throws ArgumentsAggregationException {
         return new AuthenticationMultiEndpointIT.Endpoint(
               accessor.getString(0),
               accessor.getString(1),
               accessor.getString(2),
               accessor.getString(3),
               accessor.getInteger(4),
               accessor.getBoolean(5),
               accessor.getBoolean(6),
               accessor.getBoolean(7),
               accessor.getBoolean(8),
               accessor.getString(9)
         );
      }
   }
   static class Endpoint {
      private final String protocol;
      private final String mechanism;
      private final String realm;
      private final String userPrefix;
      private final int port;
      private final boolean isAnonymous;
      private final boolean isAdmin;
      private final boolean isPlain;
      private final boolean isAlternateRealm;
      private final boolean useAuth;
      private final boolean isMechanismClearText;
      private final String contextPath;


      public Endpoint(String protocol, String mechanism, String realm, String userPrefix, int port, boolean isAnonymous, boolean isAdmin, boolean isPlain, boolean isAlternateRealm, String contextPath) {
         this.protocol = protocol;
         this.mechanism = mechanism;
         this.realm = realm;
         this.userPrefix = userPrefix;
         this.port = port;
         this.isAnonymous = isAnonymous;
         this.isAdmin = isAdmin;
         this.isPlain = isPlain;
         this.isAlternateRealm = isAlternateRealm;
         this.useAuth = !mechanism.isEmpty();
         this.isMechanismClearText = SaslMechanismInformation.Names.PLAIN.equals(mechanism) || HttpConstants.BASIC_NAME.equals(mechanism);
         this.contextPath = contextPath;
      }

      public void test() {
         if (protocol.equals("Hot Rod")) {
            testHotRod();
         } else {
            testRest();
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
            RemoteCache<String, String> cache = SERVERS.hotrod().withClientConfiguration(builder).withPort(port).withCacheMode(CacheMode.DIST_SYNC).create();
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
         RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder()
               .followRedirects(false)
               .contextPath(contextPath);
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
            RestClient client = SERVERS.rest().withClientConfiguration(builder).withPort(port).create();
            validateSuccess();
            try (RestResponse response = sync(client.cache(SERVERS.getMethodName()).post("k1", "v1"))) {
               assertEquals(204, response.getStatus());
               assertEquals(proto, response.getProtocol());
            }

            try (RestResponse response = sync(client.cache(SERVERS.getMethodName()).get("k1"))) {
               assertEquals(200, response.getStatus());
               assertEquals(proto, response.getProtocol());
               assertEquals("v1", response.getBody());
            }

            assertStatus(isAdmin ? 307 : 404, client.raw().get("/"));
            assertStatus(isAdmin ? 200 : 404, client.server().info());
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
}
