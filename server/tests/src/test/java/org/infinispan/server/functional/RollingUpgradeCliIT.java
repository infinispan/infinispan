package org.infinispan.server.functional;

import java.io.File;
import java.util.Properties;

import org.infinispan.cli.commands.CLI;
import org.infinispan.cli.impl.AeshDelegatingShell;
import org.infinispan.client.rest.RestClient;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.server.test.core.AeshTestConnection;
import org.junit.BeforeClass;

/**
 * @since 11.0
 */
public class RollingUpgradeCliIT extends RollingUpgradeIT {

   private static File workingDir;
   private static Properties properties;

   @BeforeClass
   public static void setup() {
      workingDir = new File(CommonsTestingUtil.tmpDirectory(RollingUpgradeCliIT.class));
      Util.recursiveFileRemove(workingDir);
      workingDir.mkdirs();
      properties = new Properties(System.getProperties());
      properties.put("cli.dir", workingDir.getAbsolutePath());
   }

   protected void disconnectSource(RestClient client) {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{}, properties);
         terminal.send("connect " + target.driver.getServerAddress(0).getHostAddress() + ":" + target.getSinglePort(0));
         terminal.assertContains("//containers/default]>");
         terminal.clear();
         terminal.send("migrate cluster disconnect --cache=" + CACHE_NAME);
      }
   }

   protected void doRollingUpgrade(RestClient client) {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{}, properties);
         terminal.send("connect " + target.driver.getServerAddress(0).getHostAddress() + ":" + target.getSinglePort(0));
         terminal.assertContains("//containers/default]>");
         terminal.clear();
         terminal.send("migrate cluster synchronize --cache=" + CACHE_NAME);
      }
   }
}
