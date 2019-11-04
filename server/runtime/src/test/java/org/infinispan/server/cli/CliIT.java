package org.infinispan.server.cli;

import java.net.URL;
import java.util.Arrays;

import org.aesh.terminal.utils.Config;
import org.infinispan.cli.CLI;
import org.infinispan.server.test.AeshTestConnection;
import org.infinispan.server.test.AeshTestShell;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerRuleConfigurationBuilder;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CliIT {

   @ClassRule
   public static InfinispanServerRule SERVERS = new InfinispanServerRule(new InfinispanServerRuleConfigurationBuilder("configuration/ClusteredServerTest.xml"));

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);


   @Test
   public void testCliInteractive() {
      CLI cli = new CLI();
      AeshTestConnection terminal = new AeshTestConnection();
      cli.setTerminalConnection(terminal);
      cli.run(new String[]{});

      terminal.readln("echo Hi");
      terminal.assertEquals("[disconnected]> echo Hi" + Config.getLineSeparator() + "Hi" + Config.getLineSeparator() + "[disconnected]> ");
      terminal.clear();
      terminal.readln("connect");
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

      terminal.readln("create cache --file=" + this.getClass().getResource("/cli/qcache.xml").getPath() + " qcache");
      terminal.readln("schema -u=" + this.getClass().getResource("/cli/person.proto").getPath() + " person.proto");
      terminal.clear();
      terminal.readln("cd /containers/default/schemas");
      terminal.readln("ls");
      terminal.assertContains("person.proto");
      terminal.readln("cache qcache");
      terminal.assertContains("//containers/default/caches/qcache]>");
      for (String person : Arrays.asList("jessicajones", "dannyrandy", "lukecage", "matthewmurdock")) {
         terminal.readln("put --encoding=application/json --file=" + this.getClass().getResource("/cli/" + person + ".json").getPath() + " " + person);
      }
      terminal.clear();
      terminal.readln("ls");
      for (String person : Arrays.asList("jessicajones", "dannyrandy", "lukecage", "matthewmurdock")) {
         terminal.assertContains(person);
      }
      terminal.clear();
      terminal.readln("query \"from org.infinispan.rest.search.entity.Person p where p.gender = 'MALE'\"");
      terminal.assertContains("\"total_results\" : 3,");

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

      cli.stop();
   }

   @Test
   public void testCliBatch() {
      CLI cli = new CLI();
      AeshTestShell shell = new AeshTestShell();
      cli.setShell(shell);
      cli.run(new String[]{"-f", this.getClass().getResource("/cli/batch.cli").getPath()});
      shell.assertContains("Hi CLI");
      shell.assertContains("batch1");
      cli.stop();
   }

   @Test
   public void testCliBatchPreconnect() {
      CLI cli = new CLI();
      AeshTestShell shell = new AeshTestShell();
      cli.setShell(shell);
      cli.run(new String[]{"--connect=http://localhost:11222", "-f", this.getClass().getResource("/cli/batch-preconnect.cli").getPath()});
      shell.assertContains("Hi CLI");
      shell.assertContains("batch2");
      cli.stop();
   }

   @Test
   public void testCliTasks() {
      CLI cli = new CLI();
      AeshTestConnection terminal = new AeshTestConnection();
      cli.setTerminalConnection(terminal);
      cli.run(new String[]{"--connect=http://localhost:11222"});

      terminal.readln("cd tasks");
      terminal.readln("ls");
      terminal.assertContains("@@cache@names");
      terminal.clear();
      terminal.readln("task exec @@cache@names");
      terminal.assertContains("\"___script_cache\"");
      terminal.clear();
      URL resource = this.getClass().getResource("/cli/hello.js");
      terminal.readln("task upload --file=" + resource.getPath() + " hello");
      terminal.readln("task exec hello -Pgreetee=world");
      terminal.assertContains("\"Hello world\"");
      cli.stop();
   }
}
