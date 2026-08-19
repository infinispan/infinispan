package org.infinispan.server.cli;

import static org.infinispan.server.test.core.AbstractInfinispanServerDriver.KEY_PASSWORD;

import org.infinispan.server.test.core.CliConnection;
import org.infinispan.server.test.core.InfinispanServerDriver;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.jupiter.InfinispanServerExtension;
import org.infinispan.server.test.jupiter.InfinispanServerExtensionBuilder;
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

   @Test
   public void cliClientCert() {
      InfinispanServerDriver driver = SERVERS.getServerDriver();
      CliConnection connection = SERVERS.cli().withArguments(
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
            "https://" + hostAddress() + ":11222").connection();
      connection.assertContains("//containers/default]>");
      connection.clear();
   }

   @Test
   public void connectClientCert() {
      InfinispanServerDriver driver = SERVERS.getServerDriver();
      CliConnection connection = SERVERS.cli().connection();
      connection.assertContains("[disconnected]");
      connection.send(String.format("connect -t %s -s %s -k %s -w %s --hostname-verifier=.* https://%s:11222",
            driver.getCertificateFile("ca.pfx").getAbsolutePath(),
            KEY_PASSWORD,
            driver.getCertificateFile("admin.pfx").getAbsolutePath(),
            KEY_PASSWORD,
            hostAddress()
      ));

      connection.assertContains("//containers/default]>");
      connection.clear();
   }

   private String hostAddress() {
      return SERVERS.getServerDriver().getServerAddress(0).getHostAddress();
   }
}
