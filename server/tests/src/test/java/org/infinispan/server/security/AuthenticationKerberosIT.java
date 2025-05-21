package org.infinispan.server.security;

import static org.infinispan.server.test.core.Common.HTTP_KERBEROS_MECHS;
import static org.infinispan.server.test.core.Common.HTTP_PROTOCOLS;
import static org.infinispan.server.test.core.Common.sync;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.infinispan.server.test.core.Krb5ConfPropertyExtension;
import org.infinispan.server.test.core.LdapServerListener;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.1
 **/
@Category(Security.class)
@ExtendWith(Krb5ConfPropertyExtension.class)
@Tag("embedded")
public class AuthenticationKerberosIT {
   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthenticationKerberosTest.xml")
                                    .numServers(1)
                                    .property("java.security.krb5.conf", "${infinispan.server.config.path}/krb5.conf")
                                    .addListener(new LdapServerListener(true))
                                    .build();

   static class ArgsProvider implements ArgumentsProvider {
      @Override
      public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
         List<Arguments> args = Common.SASL_KERBEROS.stream()
               .map(k -> Arguments.of("Hot Rod", k))
               .collect(Collectors.toList());

         for (Protocol protocol : HTTP_PROTOCOLS) {
            for (String mech : HTTP_KERBEROS_MECHS) {
               args.add(Arguments.of(protocol.name(), mech));
            }
         }
         return args.stream();
      }
   }

   @ParameterizedTest
   @ArgumentsSource(ArgsProvider.class)
   public void testProtocol(String protocol, String mechanism) {
      if ("Hot Rod".equals(protocol)) {
         testHotRod(mechanism);
      } else {
         testRest(Protocol.valueOf(protocol), mechanism);
      }
   }

   public void testHotRod(String mechanism) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      if (!mechanism.isEmpty()) {
         builder.security().authentication()
               .saslMechanism(mechanism)
               .serverName("datagrid")
               .callbackHandler(new VoidCallbackHandler())
               .clientSubject(Common.createSubject("admin", "INFINISPAN.ORG", "strongPassword".toCharArray()));
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

   public void testRest(Protocol protocol, String mechanism) {
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
         Exceptions.expectException(SecurityException.class, () -> SERVERS.rest().withClientConfiguration(builder).create());
      } else {
         RestClient client = SERVERS.rest().withClientConfiguration(builder).create();
         try (RestResponse response = sync(client.cache(SERVERS.getMethodName()).post("k1", "v1"))) {
            assertEquals(204, response.getStatus());
            assertEquals(protocol, response.getProtocol());
         }

         try (RestResponse response = sync(client.cache(SERVERS.getMethodName()).get("k1"))) {
            assertEquals(200, response.getStatus());
            assertEquals(protocol, response.getProtocol());
            assertEquals("v1", response.getBody());
         }
      }
   }
}
