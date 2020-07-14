package org.infinispan.server.cli;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.aesh.terminal.utils.Config;
import org.infinispan.cli.commands.CLI;
import org.infinispan.cli.impl.AeshDelegatingShell;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.server.test.core.AeshTestConnection;
import org.infinispan.server.test.core.AeshTestShell;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CliIT {

   @ClassRule
   public static InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/ClusteredServerTest.xml")
               .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private static File workingDir;

   @BeforeClass
   public static void setup() {
      workingDir = new File(CommonsTestingUtil.tmpDirectory(CliIT.class));
      Util.recursiveFileRemove(workingDir);
      workingDir.mkdirs();
   }

   @AfterClass
   public static void teardown() {
      Util.recursiveFileRemove(workingDir);
   }

   @Test
   public void testCliInteractive() {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{});

         terminal.readln("echo Hi");
         terminal.assertEquals("[disconnected]> echo Hi" + Config.getLineSeparator() + "Hi" + Config.getLineSeparator() + "[disconnected]> ");
         terminal.clear();
         terminal.readln("connect " + hostAddress() + ":11222");
         terminal.assertContains("//containers/default]>");
         terminal.clear();

         terminal.readln("create cache --template=org.infinispan.DIST_SYNC dcache");
         terminal.readln("cd caches/dcache");
         terminal.assertContains("//containers/default/caches/dcache]>");
         terminal.readln("put k1 v1");
         terminal.clear();
         terminal.readln("ls");
         terminal.assertContains("k1");
         terminal.readln("get k1");
         terminal.assertContains("v1");
         terminal.readln("put --ttl=10 k2 v2");
         terminal.clear();
         terminal.readln("describe k2");
         terminal.assertContains("\"timetoliveseconds\" : [ \"10\" ]");

         terminal.readln("create cache --file=" + getCliResource("qcache.xml").getPath() + " qcache");
         terminal.readln("schema -u=" + getCliResource("person.proto").getPath() + " person.proto");
         terminal.clear();
         terminal.readln("cd /containers/default/schemas");
         terminal.readln("ls");
         terminal.assertContains("person.proto");
         terminal.readln("cache qcache");
         terminal.assertContains("//containers/default/caches/qcache]>");
         for (String person : Arrays.asList("jessicajones", "dannyrandy", "lukecage", "matthewmurdock")) {
            terminal.readln("put --encoding=application/json --file=" + getCliResource(person + ".json").getPath() + " " + person);
         }
         terminal.clear();
         terminal.readln("ls");
         for (String person : Arrays.asList("jessicajones", "dannyrandy", "lukecage", "matthewmurdock")) {
            terminal.assertContains(person);
         }
         terminal.clear();
         terminal.readln("query \"from org.infinispan.rest.search.entity.Person p where p.gender = 'MALE'\"");
         terminal.assertContains("\"total_results\":3,");

         terminal.clear();
         terminal.readln("create counter --type=strong --storage=PERSISTENT --upper-bound=100 cnt1");
         terminal.readln("cd /containers/default/counters/cnt1");
         terminal.readln("describe");
         terminal.assertContains("\"upper-bound\" : 100");
         terminal.clear();
         terminal.readln("add");
         terminal.assertContains("1");
         terminal.clear();
         terminal.readln("reset");
         terminal.readln("ls");
         terminal.assertContains("0");
         terminal.clear();
         terminal.readln("add --delta=100");
         terminal.assertContains("100");
      }
   }

   @Test
   public void testCliBatch() {
      System.setProperty("serverAddress", hostAddress());
      AeshTestShell shell = new AeshTestShell();
      CLI.main(shell, new String[]{"-f", getCliResource("batch.cli").getPath()});
      shell.assertContains("Hi CLI running on " + System.getProperty("os.arch"));
      shell.assertContains("batch1");
   }

   @Test
   public void testCliBatchPreconnect() {
      AeshTestShell shell = new AeshTestShell();
      CLI.main(shell, new String[]{connectionString(), "-f", getCliResource("batch-preconnect.cli").getPath()});
      shell.assertContains("Hi CLI");
      shell.assertContains("batch2");
   }

   @Test
   public void testCliTasks() {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{connectionString()});
         terminal.readln("cd tasks");
         terminal.readln("ls");
         terminal.assertContains("@@cache@names");
         terminal.clear();
         terminal.readln("task exec @@cache@names");
         terminal.assertContains("\"___script_cache\"");
         terminal.clear();
         File resource = getCliResource("hello.js");
         terminal.readln("task upload --file=" + resource.getPath() + " hello");
         terminal.readln("task exec hello -Pgreetee=world");
         terminal.assertContains("\"Hello world\"");
      }
   }

   @Test
   public void testCliUploadProtobufSchemas() {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{});

         // connect
         terminal.readln("connect " + hostAddress() + ":11222");
         terminal.assertContains("//containers/default]>");
         terminal.clear();

         // upload
         terminal.readln("schema --upload=" + getCliResource("person.proto").getPath() + " person.proto");
         terminal.clear();
         terminal.readln("cd /containers/default/schemas");
         terminal.readln("ls");
         terminal.assertContains("person.proto");
      }
   }

   private String hostAddress() {
      return SERVERS.getTestServer().getDriver().getServerAddress(0).getHostAddress();
   }

   private String connectionString() {
      return String.format("--connect=http://%s:11222", hostAddress());
   }

   private File getCliResource(String resource) {
      Path dest = workingDir.toPath().resolve(resource);
      File destFile = dest.toFile();
      if (destFile.exists())
         return destFile;

      // Copy jar resources to the local working directory so that the CLI can find the files when the test is executed
      // by an external module
      try (InputStream is = getClass().getResourceAsStream("/cli/" + resource)) {
         Files.copy(is, dest);
         return dest.toFile();
      } catch (IOException e) {
         throw new IllegalStateException(e);
      }
   }
}
