package org.infinispan.server.security.authentication;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.util.Util.toHexString;
import static org.infinispan.server.test.core.Common.HTTP_MECHS;
import static org.infinispan.server.test.core.Common.HTTP_PROTOCOLS;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.sync;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.wildfly.security.mechanism._private.ElytronMessages.httpDigest;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.InfinispanServerDriver;
import org.infinispan.server.test.core.tags.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.wildfly.security.mechanism.digest.DigestUtil;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/

@Security
public class RestAuthentication {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = AuthenticationIT.SERVERS;

   static class ArgsProvider implements ArgumentsProvider {
      @Override
      public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
         List<Arguments> args = new ArrayList<>(HTTP_MECHS.size() * Common.HTTP_PROTOCOLS.size());
         for (Protocol protocol : HTTP_PROTOCOLS) {
            for (String mech : HTTP_MECHS) {
               args.add(Arguments.of(protocol, mech));
            }
         }
         return args.stream();
      }
   }

   @ParameterizedTest(name = "{1}({0})")
   @ArgumentsSource(ArgsProvider.class)
   public void testStaticResourcesAnonymously(Protocol protocol, String mechanism) throws Exception {
      InfinispanServerDriver serverDriver = SERVERS.getServerDriver();

      InetSocketAddress serverAddress = serverDriver.getServerSocket(0, 11222);
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder().followRedirects(false);
      builder.addServer().host(serverAddress.getHostString()).port(serverAddress.getPort());

      try (RestClient restClient = RestClient.forConfiguration(builder.build())) {
         assertStatus(307, restClient.raw().get("/")); // The root resource redirects to the console
      }
   }

   @ParameterizedTest(name = "{1}({0})")
   @ArgumentsSource(ArgsProvider.class)
   public void testMalformedDigestHeader(Protocol protocol, String mechanism) throws Exception {
      assumeTrue(mechanism.startsWith("DIGEST"));
      InfinispanServerDriver serverDriver = SERVERS.getServerDriver();

      InetSocketAddress serverAddress = serverDriver.getServerSocket(0, 11222);
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder().followRedirects(false);
      builder.addServer().host(serverAddress.getHostString()).port(serverAddress.getPort());

      try (RestClient restClient = RestClient.forConfiguration(builder.build());
           RestResponse response = sync(restClient.raw().get("/rest/v2/caches"))) {
         assertEquals(401, response.status());
         String auth = response.headers().get("Www-Authenticate").stream().filter(h -> h.startsWith("Digest")).findFirst().get();
         HashMap<String, byte[]> parameters = DigestUtil.parseResponse(auth.substring(7).getBytes(UTF_8), UTF_8, false, httpDigest);
         final String realm = new String(parameters.get("realm"), UTF_8);
         final String nonce = new String(parameters.get("nonce"), UTF_8);
         final String opaque = new String(parameters.get("opaque"), UTF_8);
         final String algorithm = new String(parameters.get("algorithm"), UTF_8);
         final String charset = StandardCharsets.ISO_8859_1.name();
         final MessageDigest digester = MessageDigest.getInstance(algorithm);
         final String nc = "00000001";
         final String cnonce = "00000000";
         final String username = "h4ck0rz";
         final String password = "letmein";
         final String uri = "/backdoor";
         final String s1 = username + ':' + realm + ':' + password;
         final String s2 = "GET:" + uri;
         final String hasha1 = toHexString(digester.digest(s1.getBytes(charset)));
         final String h2 = toHexString(digester.digest(s2.getBytes(charset)));
         final String digestValue = hasha1 + ':' + nonce + ':' + nc + ':' + cnonce + ":auth:" + h2;
         final String digest = toHexString(digester.digest(digestValue.getBytes(StandardCharsets.US_ASCII)));
         String authz = String.format("Digest username=\"%s\", realm=\"%s\", nonce=\"%s\", uri=\"%s\", response=\"%s\", qop=auth, nc=%s, cnonce=%s, algorithm=%s, opaque=\"%s\"", username, realm, nonce, uri, digest, nc, cnonce, algorithm, opaque);
         assertStatus(400, restClient.raw().get("/rest/v2/caches", Collections.singletonMap("Authorization", authz)));
      }
   }

   @ParameterizedTest(name = "{1}({0})")
   @ArgumentsSource(ArgsProvider.class)
   public void testRestReadWrite(Protocol protocol, String mechanism) {
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
      if (mechanism.isEmpty()) {
         Exceptions.expectException(SecurityException.class, () -> SERVERS.rest().withClientConfiguration(builder).create());
      } else {
         RestClient client = SERVERS.rest().withClientConfiguration(builder).create();
         try (RestResponse response = sync(client.cache(SERVERS.getMethodName()).post("k1", "v1"))) {
            assertEquals(204, response.status());
            assertEquals(protocol, response.protocol());
         }

         try (RestResponse response = sync(client.cache(SERVERS.getMethodName()).get("k1"))) {
            assertEquals(200, response.status());
            assertEquals(protocol, response.protocol());
            assertEquals("v1", response.body());
         }
      }
   }
}
