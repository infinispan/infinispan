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
      assertEquals(8, logAppender.size());
      assertThat(parseAccessLog(0)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCTXT", "METHOD", "auth", "STATUS", "\"CLIENT_ERROR authentication failed: Wrong credentials\"", "WHO", "-"));
      assertThat(parseAccessLog(1)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCTXT", "METHOD", "auth", "STATUS", "OK", "WHO", "-"));
      assertThat(parseAccessLog(2)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCTXT", "METHOD", "set", "STATUS", "OK", "WHO", "writer"));
      assertThat(parseAccessLog(3)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCTXT", "METHOD", "get", "STATUS", "OK", "WHO", "writer"));
      assertThat(parseAccessLog(4)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCTXT", "METHOD", "auth", "STATUS", "OK", "WHO", "-"));
      assertThat(parseAccessLog(5)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCTXT", "METHOD", "set", "STATUS", "\"CLIENT_ERROR authentication failed: ISPN000287: Unauthorized access: subject 'Subject with principal(s): [SimpleUserPrincipal [name=reader]]' lacks 'WRITE' permission\"", "WHO", "-"));
      assertThat(parseAccessLog(6)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCTXT", "METHOD", "get", "STATUS", "OK", "WHO", "reader"));
      assertThat(parseAccessLog(7)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCTXT", "METHOD", "auth", "STATUS", "\"CLIENT_ERROR authentication failed: null\"", "WHO", "-"));
   }
}
