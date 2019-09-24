package org.infinispan.server.cli;

import java.util.Arrays;

import org.aesh.terminal.utils.Config;
import org.infinispan.cli.CLI;
import org.infinispan.server.test.AeshTestConnection;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerTestConfiguration;
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
   public static InfinispanServerRule SERVERS = new InfinispanServerRule(new InfinispanServerTestConfiguration("configuration/ClusteredServerTest.xml"));

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);


   @Test
   public void testCLI() {
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
      terminal.readln("create cache --file=" + this.getClass().getResource("/cli/qcache.xml").getPath() + " qcache");
      terminal.readln("cd caches/___protobuf_metadata");
      terminal.assertContains("//containers/default/caches/___protobuf_metadata]>");
      terminal.readln("put --file=" + this.getClass().getResource("/cli/person.proto").getPath() + " person.proto");
      terminal.clear();
      terminal.readln("ls");
      terminal.assertContains("person.proto");
      terminal.readln("cache qcache");
      terminal.assertContains("//containers/default/caches/qcache]>");
      for(String person : Arrays.asList("jessicajones", "dannyrandy", "lukecage", "matthewmurdock")) {
         terminal.readln("put --encoding=application/json --file=" + this.getClass().getResource("/cli/" + person + ".json").getPath() + " " + person);
      }
      terminal.clear();
      terminal.readln("ls");
      for(String person : Arrays.asList("jessicajones", "dannyrandy", "lukecage", "matthewmurdock")) {
         terminal.assertContains(person);
      }
      terminal.clear();
      terminal.readln("query \"from org.infinispan.rest.search.entity.Person p where p.gender = 'MALE'\"");
      terminal.assertContains("\"total_results\" : 3,");
      cli.stop();
   }
}
