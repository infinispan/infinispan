package org.infinispan.server.cli;

import static org.infinispan.server.test.core.AbstractInfinispanServerDriver.KEY_PASSWORD;

import java.io.File;
import java.util.Properties;

import org.infinispan.cli.commands.CLI;
import org.infinispan.cli.impl.AeshDelegatingShell;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.server.test.core.AeshTestConnection;
import org.infinispan.server.test.core.InfinispanServerDriver;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 14.0
 **/
public class CliCertIT {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthenticationServerTrustTest.xml")
               .runMode(ServerRunMode.CONTAINER)
               .build();

   private static File workingDir;
   private static Properties properties;

   @BeforeAll
   public static void setup() {
      workingDir = new File(CommonsTestingUtil.tmpDirectory(CliCertIT.class));
      Util.recursiveFileRemove(workingDir);
      workingDir.mkdirs();
      properties = new Properties(System.getProperties());
      properties.put("cli.dir", workingDir.getAbsolutePath());
   }

   @AfterAll
   public static void teardown() {
      Util.recursiveFileRemove(workingDir);
   }

   @Test
   public void cliClientCert() {
      InfinispanServerDriver driver = SERVERS.getServerDriver();
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), properties,
               "-t",
               driver.getCertificateFile("ca.pfx").getAbsolutePath(),
               "-s",
               KEY_PASSWORD,
               "-k",
               driver.getCertificateFile("admin.pfx").getAbsolutePath(),
               "-w",
               KEY_PASSWORD,
               "--hostname-verifier",
               ".*",
               "-c",
               "https://" + hostAddress() + ":11222");
         terminal.assertContains("//containers/default]>");
         terminal.clear();
      }
   }

   @Test
   public void connectClientCert() {
      InfinispanServerDriver driver = SERVERS.getServerDriver();
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), properties);
         terminal.assertContains("[disconnected]");
         terminal.send(String.format("connect -t %s -s %s -k %s -w %s --hostname-verifier=.* https://%s:11222",
               driver.getCertificateFile("ca.pfx").getAbsolutePath(),
               KEY_PASSWORD,
               driver.getCertificateFile("admin.pfx").getAbsolutePath(),
               KEY_PASSWORD,
               hostAddress()
         ));

         terminal.assertContains("//containers/default]>");
         terminal.clear();
      }
   }

   private String hostAddress() {
      return SERVERS.getServerDriver().getServerAddress(0).getHostAddress();
   }
}
