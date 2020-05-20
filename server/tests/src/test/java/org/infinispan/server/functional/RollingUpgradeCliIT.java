package org.infinispan.server.functional;

import org.infinispan.cli.commands.CLI;
import org.infinispan.cli.impl.AeshDelegatingShell;
import org.infinispan.client.rest.RestClient;
import org.infinispan.server.test.core.AeshTestConnection;

/**
 * @since 11.0
 */
public class RollingUpgradeCliIT extends RollingUpgradeIT {

   protected void disconnectSource(RestClient client) {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{});
         terminal.readln("connect");
         terminal.assertContains("//containers/default]>");
         terminal.clear();
         terminal.readln("migrate cluster disconnect --cache=" + CACHE_NAME);
      }
   }

   protected void doRollingUpgrade(RestClient client) {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{});
         terminal.readln("connect");
         terminal.assertContains("//containers/default]>");
         terminal.clear();
         terminal.readln("migrate cluster synchronize --cache=" + CACHE_NAME);
      }
   }
}
