package org.infinispan.server.hotrod.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.Map;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.infinispan.security.Security;
import org.infinispan.server.core.AbstractAuthAccessLoggingTest;
import org.infinispan.server.core.security.simple.SimpleAuthenticator;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.server.hotrod.test.TestCallbackHandler;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.hotrod.logging.HotRodAuthAccessLoggingTest")
public class HotRodAuthAccessLoggingTest extends AbstractAuthAccessLoggingTest {
   private HotRodServer hotRodServer;

   @Override
   protected void setup() throws Exception {
      super.setup();
      SimpleAuthenticator ssap = new SimpleAuthenticator();
      ssap.addUser("writer", REALM, "writer".toCharArray());
      ssap.addUser("reader", REALM, "reader".toCharArray());
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.authentication().enable()
            .sasl()
            .authenticator(ssap)
            .addAllowedMech("CRAM-MD5")
            .serverName("localhost")
            .addMechProperty(Sasl.POLICY_NOANONYMOUS, "true");
      hotRodServer = Security.doAs(ADMIN, () -> startHotRodServer(cacheManager, HotRodTestingUtil.serverPort(), builder));
   }

   @Override
   protected String logCategory() {
      return "org.infinispan.HOTROD_ACCESS_LOG";
   }

   @Override
   protected void teardown() {
      hotRodServer.stop();
      super.teardown();
   }

   public void testHotRodAccessLog() throws SaslException {
      for (Map.Entry<String, String> user : USERS.entrySet()) {
         try (HotRodClient client = createClient(user.getKey(), user.getValue())) {
            client.put(k(0, user.getKey()), v());
            client.get(k(0, user.getKey()));
         }
      }

      assertEquals(18, logAppender.size());

      // Initial client PING
      assertThat(parseAccessLog(0)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HOTROD/2.1", "METHOD", "PING", "STATUS", "OK", "WHO", "-"));
      // Unauthenticated user
      assertThat(parseAccessLog(1)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HOTROD/2.1", "METHOD", "PUT", "STATUS", "\"ISPN006017: Operation 'PUT' requires authentication\"", "WHO", "-"));
      assertThat(parseAccessLog(2)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HOTROD/2.1", "METHOD", "GET", "STATUS", "\"ISPN006017: Operation 'GET' requires authentication\"", "WHO", "-"));
      // Initial client PING
      assertThat(parseAccessLog(3)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HOTROD/2.1", "METHOD", "PING", "STATUS", "OK", "WHO", "-"));
      // SASL authentications are always in pairs (challenge/response)
      assertThat(parseAccessLog(4)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HOTROD/2.1", "METHOD", "AUTH", "STATUS", "OK", "WHO", "-"));
      assertThat(parseAccessLog(5)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HOTROD/2.1", "METHOD", "AUTH", "STATUS", "OK", "WHO", "-"));
      // Writer
      assertThat(parseAccessLog(6)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HOTROD/2.1", "METHOD", "PUT", "STATUS", "OK", "WHO", "writer"));
      assertThat(parseAccessLog(7)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HOTROD/2.1", "METHOD", "GET", "STATUS", "OK", "WHO", "writer"));
      // Initial client PING
      assertThat(parseAccessLog(8)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HOTROD/2.1", "METHOD", "PING", "STATUS", "OK", "WHO", "-"));
      // SASL authentications are always in pairs (challenge/response)
      assertThat(parseAccessLog(9)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HOTROD/2.1", "METHOD", "AUTH", "STATUS", "OK", "WHO", "-"));
      assertThat(parseAccessLog(10)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HOTROD/2.1", "METHOD", "AUTH", "STATUS", "OK", "WHO", "-"));
      // Reader
      assertThat(parseAccessLog(11)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HOTROD/2.1", "METHOD", "PUT", "STATUS", "\"ISPN000287: Unauthorized access: subject 'Subject with principal(s): [SimpleUserPrincipal [name=reader], InetAddressPrincipal [address=127.0.0.1/127.0.0.1]]' lacks 'WRITE' permission\"", "WHO", "reader"));
      assertThat(parseAccessLog(12)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HOTROD/2.1", "METHOD", "GET", "STATUS", "OK", "WHO", "reader"));
      // Initial client PING
      assertThat(parseAccessLog(13)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HOTROD/2.1", "METHOD", "PING", "STATUS", "OK", "WHO", "-"));
      // SASL authentications are always in pairs (challenge/response)
      assertThat(parseAccessLog(14)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HOTROD/2.1", "METHOD", "AUTH", "STATUS", "OK", "WHO", "-"));
      // Failed authentication with wrong password
      assertThat(parseAccessLog(15)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HOTROD/2.1", "METHOD", "AUTH", "STATUS", "\"Authentication failure\"", "WHO", "-"));
      assertThat(parseAccessLog(16)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HOTROD/2.1", "METHOD", "PUT", "STATUS", "\"ISPN006017: Operation 'PUT' requires authentication\"", "WHO", "-"));
      assertThat(parseAccessLog(17)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HOTROD/2.1", "METHOD", "GET", "STATUS", "\"ISPN006017: Operation 'GET' requires authentication\"", "WHO", "-"));
   }

   private HotRodClient createClient(String username, String password) throws SaslException {
      HotRodClient client = new HotRodClient(hotRodServer.getHost(), hotRodServer.getPort(), "default", (byte) 21);
      if (!username.isEmpty()) {
         SaslClient sc = Sasl.createSaslClient(new String[]{"CRAM-MD5"}, null, "hotrod", "localhost", Collections.emptyMap(),
               new TestCallbackHandler(username, REALM, password));
         client.auth(sc);
      }
      return client;
   }
}
