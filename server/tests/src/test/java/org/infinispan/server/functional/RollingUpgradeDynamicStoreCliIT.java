package org.infinispan.server.functional;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.infinispan.cli.commands.CLI;
import org.infinispan.cli.impl.AeshDelegatingShell;
import org.infinispan.client.rest.RestClient;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.server.test.core.AeshTestConnection;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * @since 13.0
 */
public class RollingUpgradeDynamicStoreCliIT extends RollingUpgradeDynamicStoreIT {

   private static File workingDir;
   private static Properties properties;
   private static Path dest;
   private static final String REMOTE_STORE_CFG_FILE = "remote-store.json";

   @BeforeClass
   public static void setup() {
      workingDir = new File(CommonsTestingUtil.tmpDirectory(RollingUpgradeDynamicStoreCliIT.class));
      Util.recursiveFileRemove(workingDir);
      workingDir.mkdirs();
      properties = new Properties(System.getProperties());
      properties.put("cli.dir", workingDir.getAbsolutePath());
      dest = workingDir.toPath().resolve(REMOTE_STORE_CFG_FILE);
      try (InputStream is = RollingUpgradeDynamicStoreCliIT.class.getResourceAsStream("/cli/" + REMOTE_STORE_CFG_FILE)) {
         Files.copy(is, dest);
      } catch (IOException e) {
         throw new IllegalStateException(e);
      }
   }

   @AfterClass
   public static void teardown() {
      Util.recursiveFileRemove(workingDir);
   }

   @Override
   protected void connectTargetCluster() {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{}, properties);
         terminal.readln("connect 127.0.0.1:" + target.getSinglePort(0));
         terminal.assertContains("//containers/default]>");
         terminal.clear();
         terminal.readln("migrate cluster connect --file=" + dest + " --cache=" + CACHE_NAME);
      }
   }

   @Override
   protected void doRollingUpgrade(RestClient client) {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{}, properties);
         terminal.readln("connect 127.0.0.1:" + target.getSinglePort(0));
         terminal.assertContains("//containers/default]>");
         terminal.clear();
         terminal.readln("migrate cluster synchronize --cache=" + CACHE_NAME);
      }
   }

   @Override
   protected void disconnectSource(RestClient client) {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{}, properties);
         terminal.readln("connect 127.0.0.1:" + target.getSinglePort(0));
         terminal.assertContains("//containers/default]>");
         terminal.clear();
         terminal.readln("migrate cluster disconnect --cache=" + CACHE_NAME);
      }
   }
}
