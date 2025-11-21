package org.infinispan.server.memcached.logging;

import java.util.List;

import org.infinispan.server.memcached.configuration.MemcachedProtocol;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.memcached.logging.MemcachedBinaryAuthAccessLoggingTest")
public class MemcachedBinaryAuthAccessLoggingTest extends MemcachedBaseAuthAccessLoggingTest {

   @Override
   protected List<String> regexes() {
      return List.of(
            // First, authenticated client submits set operation.
            "^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"SASL_AUTH /- MCBIN\" OK \\d+ \\d+ \\d+$",
            "^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"SASL_STEP /- MCBIN\" OK \\d+ \\d+ \\d+$",
            "^127\\.0\\.0\\.1 user \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"SET /\\[B0x6B MCBIN\" OK \\d+ \\d+ \\d+$",

            // Unauthenticated client tries to submit operation.
            // The "no matching user found" is a test only because we use SimpleSaslAuthenticator implementation during the tests.
            "^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"SASL_AUTH /- MCBIN\" OK \\d+ \\d+ \\d+$",
            "^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"SASL_STEP /- MCBIN\" javax.security.sasl.AuthenticationException: No matching user found \\d+ \\d+ \\d+$"
      );
   }

   @Override
   protected MemcachedProtocol getProtocol() {
      return MemcachedProtocol.BINARY;
   }
}
