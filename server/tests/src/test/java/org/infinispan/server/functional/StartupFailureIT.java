package org.infinispan.server.functional;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.net.ServerSocket;

import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Dan Berindei
 * @since 14
 **/
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(StartupFailureIT.Extension.class)
@Tag("embedded")

public class StartupFailureIT {

   @RegisterExtension
   public static final InfinispanServerExtension SERVER =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(1)
               .runMode(ServerRunMode.EMBEDDED)
               .build();

   @Test
   public void testAddressAlreadyBound() {
      assertFalse(SERVER.getServerDriver().isRunning(0));
   }

   static class Extension implements AfterAllCallback, BeforeAllCallback {

      ServerSocket socket;

      @Override
      public void beforeAll(ExtensionContext context) throws Exception {
         socket = new ServerSocket(11222);
      }

      @Override
      public void afterAll(ExtensionContext context) throws Exception {
         if (socket != null)
            socket.close();
      }
   }
}
