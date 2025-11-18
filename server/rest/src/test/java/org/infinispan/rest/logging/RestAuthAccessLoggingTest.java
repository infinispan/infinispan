package org.infinispan.rest.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.rest.authentication.impl.BasicAuthenticator;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.rest.resources.security.SimpleSecurityDomain;
import org.infinispan.security.Security;
import org.infinispan.server.core.AbstractAuthAccessLoggingTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.RestAccessLoggingTest")
public class RestAuthAccessLoggingTest extends AbstractAuthAccessLoggingTest {
   public static final String REALM = "ApplicationRealm";
   private RestServerHelper restServer;

   @Override
   protected String logCategory() {
      return "org.infinispan.REST_ACCESS_LOG";
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      restServer = new RestServerHelper(cacheManager);
      BasicAuthenticator basicAuthenticator = new BasicAuthenticator(new SimpleSecurityDomain(ADMIN, READER, WRITER), REALM);
      restServer.withAuthenticator(basicAuthenticator);
      Security.doAs(ADMIN, () -> restServer.start(TestResourceTracker.getCurrentTestShortName()));
   }

   @Override
   protected void teardown() {
      try {
         restServer.stop();
      } catch (Exception ignored) {
      }
      super.teardown();
   }

   public void testRestAccessLog() throws Exception {
      for (Map.Entry<String, String> user : USERS.entrySet()) {
         try (RestClient client = createRestClient(user.getKey(), user.getValue())) {
            await(client.cache("default").put(k(0, user.getKey()), v()));
            await(client.cache("default").get(k(0, user.getKey())));
         }
      }
      restServer.stop();

      assertEquals(14, logAppender.size());

      // ANONYMOUS PUT
      assertThat(parseAccessLog(0)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HTTP/1.1", "METHOD", "PUT", "STATUS", "401", "WHO", "-"));
      // ANONYMOUS GET
      assertThat(parseAccessLog(1)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HTTP/1.1", "METHOD", "GET", "STATUS", "401", "WHO", "-"));
      // WRITER PUT, BEFORE AUTH
      assertThat(parseAccessLog(2)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HTTP/1.1", "METHOD", "PUT", "STATUS", "401", "WHO", "-"));
      // WRITER PUT, AFTER AUTH
      assertThat(parseAccessLog(3)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HTTP/1.1", "METHOD", "PUT", "STATUS", "204", "WHO", "writer"));
      // WRITER GET, BEFORE AUTH
      assertThat(parseAccessLog(4)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HTTP/1.1", "METHOD", "GET", "STATUS", "401", "WHO", "writer"));
      // WRITER GET, AFTER AUTH
      assertThat(parseAccessLog(5)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HTTP/1.1", "METHOD", "GET", "STATUS", "200", "WHO", "writer"));
      // READER PUT, BEFORE AUTH
      assertThat(parseAccessLog(6)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HTTP/1.1", "METHOD", "PUT", "STATUS", "401", "WHO", "-"));
      // READER PUT, AFTER AUTH
      assertThat(parseAccessLog(7)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HTTP/1.1", "METHOD", "PUT", "STATUS", "403", "WHO", "reader"));
      // READER GET, BEFORE AUTH
      assertThat(parseAccessLog(8)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HTTP/1.1", "METHOD", "GET", "STATUS", "401", "WHO", "reader"));
      // READER GET, AFTER AUTH
      assertThat(parseAccessLog(9)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HTTP/1.1", "METHOD", "GET", "STATUS", "404", "WHO", "reader"));
      // WRONG PUT, BEFORE AUTH
      assertThat(parseAccessLog(10)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HTTP/1.1", "METHOD", "PUT", "STATUS", "401", "WHO", "-"));
      // WRONG PUT, AFTER AUTH
      assertThat(parseAccessLog(11)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HTTP/1.1", "METHOD", "PUT", "STATUS", "403", "WHO", "-"));
      // WRONG GET, BEFORE AUTH
      assertThat(parseAccessLog(12)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HTTP/1.1", "METHOD", "GET", "STATUS", "401", "WHO", "-"));
      // WRONG GET, AFTER AUTH
      assertThat(parseAccessLog(13)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "HTTP/1.1", "METHOD", "GET", "STATUS", "403", "WHO", "-"));
   }

   private RestClient createRestClient(String username, String password) {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.addServer().host(restServer.getHost()).port(restServer.getPort()).protocol(Protocol.HTTP_11).pingOnCreate(false);
      if (!username.isEmpty()) {
         builder.security().authentication().enable().username(username).password(password);
      }
      return RestClient.forConfiguration(builder.build());
   }
}
