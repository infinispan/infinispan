package org.infinispan.server.memcached.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.infinispan.server.memcached.configuration.MemcachedProtocol;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.memcached.logging.MemcachedTextAuthAccessLoggingTest")
public class MemcachedTextAuthAccessLoggingTest extends MemcachedBaseAuthAccessLoggingTest {

   @Override
   protected MemcachedProtocol getProtocol() {
      return MemcachedProtocol.TEXT;
   }

   @Override
   protected void verifyLogs() {
      int i = 0;
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCTXT", "METHOD", "auth", "STATUS", "\"CLIENT_ERROR authentication failed: Wrong credentials\"", "WHO", "-"));
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCTXT", "METHOD", "get", "STATUS", "\"CLIENT_ERROR authentication failed: Forbidden\"", "WHO", "-"));
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCTXT", "METHOD", "auth", "STATUS", "OK", "WHO", "-"));
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCTXT", "METHOD", "set", "STATUS", "OK", "WHO", "writer"));
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCTXT", "METHOD", "get", "STATUS", "OK", "WHO", "writer"));
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCTXT", "METHOD", "auth", "STATUS", "OK", "WHO", "-"));
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCTXT", "METHOD", "set", "STATUS", "\"CLIENT_ERROR authentication failed: ISPN000287: Unauthorized access: subject 'Subject with principal(s): [SimpleUserPrincipal [name=reader]]' lacks 'WRITE' permission\"", "WHO", "reader"));
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCTXT", "METHOD", "auth", "STATUS", "OK", "WHO", "-"));
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCTXT", "METHOD", "get", "STATUS", "OK", "WHO", "reader"));
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCTXT", "METHOD", "auth", "STATUS", "\"CLIENT_ERROR authentication failed: Authentication failure\"", "WHO", "-"));
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCTXT", "METHOD", "auth", "STATUS", "\"CLIENT_ERROR authentication failed: Authentication failure\"", "WHO", "-"));
      assertEquals(i, logAppender.size());
   }
}
