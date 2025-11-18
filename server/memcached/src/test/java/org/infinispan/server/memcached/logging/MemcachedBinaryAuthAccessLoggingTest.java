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
      assertEquals(11, logAppender.size());
      // Unauthenticated SET
      assertThat(parseAccessLog(0)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "SET", "STATUS", "\"Forbidden\"", "WHO", "-"));
      // Unauthenticated GET
      assertThat(parseAccessLog(1)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "GET", "STATUS", "\"Forbidden\"", "WHO", "-"));
      // Auth writer
      assertThat(parseAccessLog(2)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "SASL_AUTH", "STATUS", "OK", "WHO", "-"));
      assertThat(parseAccessLog(3)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "SASL_STEP", "STATUS", "OK", "WHO", "-"));
      // Writer SET
      assertThat(parseAccessLog(4)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "SET", "STATUS", "OK", "WHO", "writer"));
      // Writer GET
      assertThat(parseAccessLog(5)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "GET", "STATUS", "OK", "WHO", "writer"));
      // Auth reader
      assertThat(parseAccessLog(6)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "SASL_AUTH", "STATUS", "OK", "WHO", "-"));
      assertThat(parseAccessLog(7)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "SASL_STEP", "STATUS", "OK", "WHO", "-"));

      assertThat(parseAccessLog(8)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "SET", "STATUS", "\"ISPN000287: Unauthorized access: subject 'Subject with principal(s): [SimpleUserPrincipal [name=reader], InetAddressPrincipal [address=127.0.0.1/127.0.0.1]]' lacks 'WRITE' permission\"", "WHO", "reader"));
      assertThat(parseAccessLog(9)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "SASL_AUTH", "STATUS", "OK", "WHO", "-"));
      assertThat(parseAccessLog(10)).containsAllEntriesOf(Map.of("IP", "127.0.0.1", "PROTOCOL", "MCBIN", "METHOD", "SASL_STEP", "STATUS", "\"Authentication failure\"", "WHO", "-"));
   }
}
