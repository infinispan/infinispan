package org.infinispan.server.cli;

import static org.infinispan.lock.impl.ClusteredLockModuleLifecycle.CLUSTERED_LOCK_CACHE_NAME;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

import org.aesh.terminal.utils.Config;
import org.infinispan.cli.commands.CLI;
import org.infinispan.cli.impl.AeshDelegatingShell;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.AeshTestConnection;
import org.infinispan.server.test.core.AeshTestShell;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.ServerRunMode;
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
         InfinispanServerRuleBuilder.config("configuration/AuthorizationImplicitTest.xml")
               .mavenArtifacts(Common.NASHORN_DEPS)
               .runMode(ServerRunMode.CONTAINER)
               .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private static File workingDir;
   private static Properties properties;

   @BeforeClass
   public static void setup() {
      workingDir = new File(CommonsTestingUtil.tmpDirectory(CliIT.class));
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
   public void testCliInteractive() {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{}, properties);

         terminal.send("echo Hi");
         terminal.assertEquals("[disconnected]> echo Hi" + Config.getLineSeparator() + "Hi" + Config.getLineSeparator() + "[disconnected]> ");
         terminal.clear();
         connect(terminal);
         terminal.send("stats");
         terminal.assertContains("required_minimum_number_of_nodes");
         terminal.clear();
         terminal.send("create cache --template=org.infinispan.DIST_SYNC dcache");
         terminal.send("cd caches/dcache");
         terminal.assertContains("//containers/default/caches/dcache]>");
         terminal.send("put k1 v1");
         terminal.clear();
         terminal.send("ls");
         terminal.assertContains("k1");
         terminal.send("get k1");
         terminal.assertContains("v1");
         terminal.send("put --ttl=10 k2 v2");
         terminal.clear();
         terminal.send("describe k2");
         terminal.assertContains("\"timetoliveseconds\" : [ \"10\" ]");

         terminal.send("schema upload -f=" + getCliResource("person.proto").getPath() + " person.proto");
         terminal.send("create cache --file=" + getCliResource("qcache.xml").getPath() + " qcache");
         terminal.clear();
         terminal.send("cd /containers/default/schemas");
         terminal.send("ls");
         terminal.assertContains("person.proto");
         terminal.send("cache qcache");
         terminal.assertContains("//containers/default/caches/qcache]>");
         for (String person : Arrays.asList("jessicajones", "dannyrandy", "lukecage", "matthewmurdock")) {
            terminal.send("put --encoding=application/json --file=" + getCliResource(person + ".json").getPath() + " " + person);
         }
         terminal.clear();
         terminal.send("ls");
         for (String person : Arrays.asList("jessicajones", "dannyrandy", "lukecage", "matthewmurdock")) {
            terminal.assertContains(person);
         }
         terminal.clear();
         terminal.send("query \"from org.infinispan.rest.search.entity.Person p where p.gender = 'MALE'\"");
         terminal.assertContains("\"total_results\":3,");
         terminal.clear();

         terminal.send("index stats qcache");
         terminal.assertContains("\"slowest\" : \"from org.infinispan.rest.search.entity.Person p where p.gender = 'MALE'\"");
         terminal.clear();
         terminal.send("index clear qcache");
         terminal.send("index reindex qcache");
         terminal.send("index clear-stats qcache");
         terminal.assertNotContains("Error");
         terminal.clear();

         terminal.send("stats");
         terminal.assertContains("required_minimum_number_of_nodes");

         // COUNTERS
         terminal.send("create counter --type=strong --storage=PERSISTENT --upper-bound=100 cnt1");
         terminal.send("cd /containers/default/counters/cnt1");
         terminal.send("describe");
         terminal.assertContains("\"upper-bound\" : \"100\"");
         terminal.clear();
         terminal.send("add");
         terminal.assertContains("1");
         terminal.clear();
         terminal.send("reset");
         terminal.send("ls");
         terminal.assertContains("0");
         terminal.clear();
         terminal.send("add --delta=100");
         terminal.assertContains("100");

         // ALTER CACHE
         terminal.send("create cache --file=" + getCliResource("xcache.xml").getPath() + " xcache");
         terminal.send("describe /containers/default/caches/xcache");
         terminal.assertContains("\"lifespan\" : \"60000\"");
         terminal.assertContains("\"max-count\" : \"1000\"");
         terminal.clear();
         terminal.send("alter cache --file=" + getCliResource("xcache-alter.xml").getPath() + " xcache");
         terminal.send("describe /containers/default/caches/xcache");
         terminal.assertContains("\"lifespan\" : \"30000\"");
         terminal.assertContains("\"max-count\" : \"2000\"");
         terminal.clear();
         terminal.send("alter cache xcache --attribute=memory.max-count --value=5000");
         terminal.send("describe /containers/default/caches/xcache");
         terminal.assertContains("\"lifespan\" : \"30000\"");
         terminal.assertContains("\"max-count\" : \"5000\"");
         terminal.clear();
      }
   }

   @Test
   public void testCliBatch() {
      System.setProperty("serverAddress", hostAddress());
      AeshTestShell shell = new AeshTestShell();
      CLI.main(shell, new String[]{"-f", getCliResource("batch.cli").getPath()}, properties);
      shell.assertContains("Hi CLI running on " + System.getProperty("os.arch"));
      shell.assertContains("batch1");
   }

   @Test
   public void testCliBatchError() {
      System.setProperty("serverAddress", hostAddress());
      AeshTestShell shell = new AeshTestShell();
      CLI.main(shell, new String[]{"-f", getCliResource("batch-error.cli").getPath()}, properties);
      shell.assertContains("Hi CLI running on " + System.getProperty("os.arch"));
      shell.assertContains("Error executing line 2");
   }

   @Test
   public void testCliBatchPreconnect() {
      AeshTestShell shell = new AeshTestShell();
      CLI.main(shell, new String[]{"-c", connectionUrl(), "-f", getCliResource("batch-preconnect.cli").getPath()}, properties);
      shell.assertContains("Hi CLI");
      shell.assertContains("batch2");
   }

   @Test
   public void testCliTasks() {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{"-c", connectionUrl()}, properties);
         connect(terminal);

         terminal.send("cd tasks");
         terminal.send("ls");
         terminal.assertContains("@@cache@names");
         terminal.clear();
         terminal.send("task exec @@cache@names");
         terminal.assertContains("\"___script_cache\"");
         terminal.clear();
         File resource = getCliResource("hello.js");
         terminal.send("task upload --file=" + resource.getPath() + " hello");
         terminal.send("task exec hello -Pgreetee=world");
         terminal.assertContains("\"Hello world\"");
      }
   }

   @Test
   public void testCliCredentials() {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{}, properties);
         String keyStore = Paths.get(System.getProperty("build.directory", ""), "key.store").toAbsolutePath().toString();

         terminal.send("credentials add --path=" + keyStore + " --password=secret --credential=credential password");
         terminal.send("credentials add --path=" + keyStore + " --password=secret --credential=credential another");
         terminal.clear();
         terminal.send("credentials ls --path=" + keyStore + " --password=secret");
         terminal.assertContains("password");
         terminal.assertContains("another");
      }
   }

   @Test
   public void testCliAuthorization() {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{}, properties);
         connect(terminal);
         terminal.send("user roles ls");
         terminal.assertContains("\"admin\"");
         terminal.send("user roles create --permissions=ALL_WRITE wizard");
         terminal.send("user roles create --permissions=ALL_READ cleric");
         terminal.clear();
         terminal.send("user roles ls");
         terminal.assertContains("\"wizard\"");
         terminal.assertContains("\"cleric\"");
         terminal.send("user roles grant --roles=wizard,cleric,admin admin");
         terminal.clear();
         terminal.send("user roles ls admin");
         terminal.assertContains("\"wizard\"");
         terminal.assertContains("\"cleric\"");
         terminal.send("user roles deny --roles=cleric admin");
         terminal.clear();
         terminal.send("user roles ls admin");
         terminal.assertContains("\"wizard\"");
         terminal.assertNotContains("\"cleric\"");
         terminal.send("user roles remove wizard");
         terminal.clear();
         terminal.send("user roles ls");
         terminal.assertContains("\"cleric\"");
         terminal.assertNotContains("\"wizard\"");
      }
   }

   private void connect(AeshTestConnection terminal) {
      connect(terminal, TestUser.ADMIN);
   }

   private void connect(AeshTestConnection terminal, TestUser user) {
      // connect
      terminal.send("connect -u " + user.getUser() + " -p " + user.getPassword() + " " + hostAddress() + ":11222");
      terminal.assertContains("//containers/default]>");
      terminal.clear();
   }

   @Test
   public void testCliUploadProtobufSchemas() {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{}, properties);

         // connect
         connect(terminal);

         // upload
         terminal.send("schema upload --file=" + getCliResource("person.proto").getPath() + " person.proto");
         terminal.assertContains("\"error\" : null");
         terminal.clear();
         terminal.send("cd /containers/default/schemas");
         terminal.send("ls");
         terminal.assertContains("person.proto");
         terminal.clear();
         terminal.send("schema ls");
         terminal.assertContains("person.proto");
         terminal.send("schema get person.proto");
         terminal.assertContains("PhoneNumber");
         terminal.send("schema remove person.proto");
         terminal.clear();
         terminal.send("schema ls");
         terminal.assertContains("[]");
      }
   }

   @Test
   public void testCliHttpBenchmark() {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{}, properties);

         // no cache
         terminal.send("benchmark " + connectionUrl());
         terminal.assertContains("java.lang.IllegalArgumentException: Could not find cache");
      }
   }

   @Test
   public void testCliConfigPersistence() {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{}, properties);

         terminal.send("config set autoconnect-url " + connectionUrl());
         terminal.clear();
         terminal.send("config get autoconnect-url");
         terminal.assertContains(connectionUrl());
      }
      // Close and recreate the CLI so that auto-connection kicks in
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{}, properties);
         terminal.assertContains("//containers/default]>");
         terminal.send("config set autoconnect-url");
      }
   }

   @Test
   public void testCliCacheAvailability() {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{}, properties);

         connect(terminal);
         terminal.send("availability " + CLUSTERED_LOCK_CACHE_NAME);
         terminal.assertContains("AVAILABLE");
         terminal.send("availability --mode=DEGRADED_MODE " + CLUSTERED_LOCK_CACHE_NAME);
         terminal.send("availability " + CLUSTERED_LOCK_CACHE_NAME);
         terminal.assertContains("DEGRADED_MODE");
         terminal.send("availability --mode=AVAILABILITY " + CLUSTERED_LOCK_CACHE_NAME);
         terminal.send("availability " + CLUSTERED_LOCK_CACHE_NAME);
         terminal.assertContains("AVAILABLE");
      }
   }

   private String hostAddress() {
      return SERVERS.getTestServer().getDriver().getServerAddress(0).getHostAddress();
   }

   private String connectionUrl() {
      return connectionUrl(TestUser.ADMIN);
   }

   private String connectionUrl(TestUser user) {
      return String.format("http://%s:%s@%s:11222", user.getUser(), user.getPassword(), hostAddress());
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
