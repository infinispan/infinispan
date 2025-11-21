package org.infinispan.server.memcached.logging;

import java.util.List;

import org.infinispan.server.memcached.configuration.MemcachedProtocol;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.memcached.logging.MemcachedTextAuthAccessLoggingTest")
public class MemcachedTextAuthAccessLoggingTest extends MemcachedBaseAuthAccessLoggingTest {

   @Override
   protected List<String> regexes() {
      return List.of(
            // First, authenticated client submits set operation.
            "^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"auth /- MCTXT\" OK \\d+ \\d+ \\d+$",
            "^127\\.0\\.0\\.1 user \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"set /k MCTXT\" OK \\d+ \\d+ \\d+$",

            // Unauthenticated client tries to submit operation.
            "^127\\.0\\.0\\.1 - \\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+ [+-]?\\d*] \"auth /- MCTXT\" CLIENT_ERROR authentication failed: Wrong credentials \\d+ \\d+ \\d+$"
      );
   }

   @Override
   protected MemcachedProtocol getProtocol() {
      return MemcachedProtocol.TEXT;
   }
}
