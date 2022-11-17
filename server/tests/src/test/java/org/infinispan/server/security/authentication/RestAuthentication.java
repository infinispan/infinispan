package org.infinispan.server.security.authentication;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.util.Util.toHexString;
import static org.infinispan.server.test.core.Common.HTTP_MECHS;
import static org.infinispan.server.test.core.Common.HTTP_PROTOCOLS;
import static org.infinispan.server.test.core.Common.sync;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static org.wildfly.security.mechanism._private.ElytronMessages.httpDigest;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.InfinispanServerDriver;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.wildfly.security.mechanism.digest.DigestUtil;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/

@RunWith(Parameterized.class)
@Category(Security.class)
public class RestAuthentication {

   @ClassRule
   public static InfinispanServerRule SERVERS = AuthenticationIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private final Protocol protocol;
   private final String mechanism;

   @Parameterized.Parameters(name = "{1}({0})")
   public static Collection<Object[]> data() {
      List<Object[]> params = new ArrayList<>(Common.HTTP_MECHS.size() * Common.HTTP_PROTOCOLS.size());
      for (Protocol protocol : HTTP_PROTOCOLS) {
         for (Object[] mech : HTTP_MECHS) {
            params.add(new Object[]{protocol, mech[0]});
         }
      }
      return params;
   }

   public RestAuthentication(Protocol protocol, String mechanism) {
      this.protocol = protocol;
      this.mechanism = mechanism;
   }

   @Test
   public void testStaticResourcesAnonymously() {
      InfinispanServerDriver serverDriver = SERVERS.getServerDriver();

      InetSocketAddress serverAddress = serverDriver.getServerSocket(0, 11222);
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder().followRedirects(false);
      builder.addServer().host(serverAddress.getHostString()).port(serverAddress.getPort());

      RestClient restClient = RestClient.forConfiguration(builder.build());

      RestResponse response = sync(restClient.raw().get("/"));
      assertEquals(307, response.getStatus()); // The root resource redirects to the console
   }

   @Test
   public void testMalformedDigestHeader() throws Exception {
      assumeTrue(mechanism.startsWith("DIGEST"));
      InfinispanServerDriver serverDriver = SERVERS.getServerDriver();

      InetSocketAddress serverAddress = serverDriver.getServerSocket(0, 11222);
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder().followRedirects(false);
      builder.addServer().host(serverAddress.getHostString()).port(serverAddress.getPort());

      RestClient restClient = RestClient.forConfiguration(builder.build());
      RestResponse response = sync(restClient.raw().get("/rest/v2/caches"));
      assertEquals(401, response.getStatus());
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
      final String digest = toHexString(digester.digest(digestValue.getBytes(StandardCharsets.US_ASCII.toString())));
      String authz = String.format("Digest username=\"%s\", realm=\"%s\", nonce=\"%s\", uri=\"%s\", response=\"%s\", qop=auth, nc=%s, cnonce=%s, algorithm=%s, opaque=\"%s\"", username, realm, nonce, uri, digest, nc, cnonce, algorithm, opaque);
      response = sync(restClient.raw().get("/rest/v2/caches", Collections.singletonMap("Authorization", authz)));
      assertEquals(400, response.getStatus());
   }

   @Test
   public void testRestReadWrite() {
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
         Exceptions.expectException(SecurityException.class, () -> SERVER_TEST.rest().withClientConfiguration(builder).create());
      } else {
         RestClient client = SERVER_TEST.rest().withClientConfiguration(builder).create();
         RestResponse response = sync(client.cache(SERVER_TEST.getMethodName()).post("k1", "v1"));
         assertEquals(204, response.getStatus());
         assertEquals(protocol, response.getProtocol());
         response = sync(client.cache(SERVER_TEST.getMethodName()).get("k1"));
         assertEquals(200, response.getStatus());
         assertEquals(protocol, response.getProtocol());
         assertEquals("v1", response.getBody());
      }
   }
}
