package org.infinispan.server.memcached.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.infinispan.server.memcached.configuration.MemcachedProtocol;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.memcached.logging.MemcachedBinaryAuthAccessLoggingTest")
public class MemcachedBinaryAuthAccessLoggingTest extends MemcachedBaseAuthAccessLoggingTest {

   @Override
   protected MemcachedProtocol getProtocol() {
      return MemcachedProtocol.BINARY;
   }

   @Override
   protected void verifyLogs() {
      int i = 0;
      // Unauthenticated SET
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "SET", "STATUS", "\"Forbidden\"", "WHO", "-"));
      // Unauthenticated GET
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "GET", "STATUS", "\"Forbidden\"", "WHO", "-"));
      // Auth writer
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "SASL_AUTH", "STATUS", "OK", "WHO", "-"));
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "SASL_STEP", "STATUS", "OK", "WHO", "-"));
      // Writer SET
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "SET", "STATUS", "OK", "WHO", "writer"));
      // Writer GET
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "GET", "STATUS", "OK", "WHO", "writer"));
      // Auth reader
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "SASL_AUTH", "STATUS", "OK", "WHO", "-"));
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "SASL_STEP", "STATUS", "OK", "WHO", "-"));

      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "SET", "STATUS", "\"ISPN000287: Unauthorized access: subject 'Subject with principal(s): [SimpleUserPrincipal [name=reader], InetAddressPrincipal [address=127.0.0.1/127.0.0.1]]' lacks 'WRITE' permission\"", "WHO", "reader"));
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "SASL_AUTH", "STATUS", "OK", "WHO", "-"));
      assertThat(parseAccessLog(i++)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "SASL_STEP", "STATUS", "\"Authentication failure\"", "WHO", "-"));

      assertEquals(i, logAppender.size());
   }
}
