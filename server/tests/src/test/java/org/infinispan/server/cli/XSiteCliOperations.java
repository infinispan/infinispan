package org.infinispan.server.cli;

import java.io.File;
import java.util.Properties;
import java.util.function.Consumer;

import org.infinispan.cli.commands.CLI;
import org.infinispan.cli.impl.AeshDelegatingShell;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.server.functional.XSiteIT;
import org.infinispan.server.test.core.AeshTestConnection;
import org.infinispan.server.test.core.TestServer;
import org.infinispan.server.test.junit4.InfinispanXSiteServerRule;
import org.infinispan.server.test.junit4.InfinispanXSiteServerTestMethodRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * CLI test for 'site' command
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
public class XSiteCliOperations {

   @ClassRule
   public static final InfinispanXSiteServerRule SERVERS = XSiteIT.SERVERS;

   @Rule
   public InfinispanXSiteServerTestMethodRule SERVER_TEST = new InfinispanXSiteServerTestMethodRule(SERVERS);

   private static File workingDir;
   private static Properties properties;

   @BeforeClass
   public static void setup() {
      workingDir = new File(CommonsTestingUtil.tmpDirectory(XSiteCliOperations.class));
      Util.recursiveFileRemove(workingDir);
      workingDir.mkdirs();
      properties = new Properties(System.getProperties());
      properties.put("cli.dir", workingDir.getAbsolutePath());
   }

   @AfterClass
   public static void teardown() {
      Util.recursiveFileRemove(workingDir);
   }

   @Test
   public void testSiteView() {
      doWithTerminal(terminal -> {
         connect(terminal, XSiteIT.LON);

         terminal.readln("site name");
         terminal.assertContains(XSiteIT.LON);
         terminal.clear();

         terminal.readln("site view");
         terminal.assertContains(XSiteIT.LON);
         terminal.assertContains(XSiteIT.NYC);
         terminal.clear();

         disconnect(terminal);
         connect(terminal, XSiteIT.NYC);

         terminal.readln("site name");
         terminal.assertContains(XSiteIT.NYC);
         terminal.clear();

         terminal.readln("site view");
         terminal.assertContains(XSiteIT.LON);
         terminal.assertContains(XSiteIT.NYC);
         terminal.clear();
      });
   }

   private void connect(AeshTestConnection terminal, String site) {
      // connect
      terminal.readln("connect " + hostAndPort(site));
      terminal.assertContains("//containers/default]>");
      terminal.clear();
   }

   private void disconnect(AeshTestConnection terminal) {
      // connect
      terminal.readln("disconnect");
      terminal.clear();
   }

   private String hostAndPort(String site) {
      for (TestServer server : SERVERS.getTestServers()) {
         if (site.equals(server.getSiteName())) {
            String host = server.getDriver().getServerAddress(0).getHostAddress();
            int port = server.getDriver().getServerSocket(0, 11222).getPort();
            return host + ":" + port;
         }
      }
      throw new IllegalStateException("Site " + site + " not found.");
   }

   private void doWithTerminal(Consumer<AeshTestConnection> consumer) {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{}, properties);
         consumer.accept(terminal);
      }
   }
}
