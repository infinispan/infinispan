package org.infinispan.server.cli;

import static org.infinispan.commons.internal.InternalCacheNames.SCRIPT_CACHE_NAME;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.aesh.terminal.utils.Config;
import org.infinispan.server.test.api.CliTestDriver;
import org.infinispan.server.test.core.CliConnection;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.jupiter.InfinispanServerExtension;
import org.infinispan.server.test.jupiter.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CliIT {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthorizationImplicitTest.xml")
               .mavenArtifacts(Common.NASHORN_DEPS)
               .runMode(ServerRunMode.CONTAINER)
               .numServers(1)
               .build();

   @Test
   public void testCliInteractive() {
      CliTestDriver cli = SERVERS.cli();
      CliConnection connection = cli.connection();
      connection.send("echo Hi");
      connection.assertEquals("[disconnected]> echo Hi" + Config.getLineSeparator() + "Hi" + Config.getLineSeparator() + "[disconnected]> ");
      cli.connect();
      connection.send("stats");
      connection.assertContains("required_minimum_number_of_nodes");
      connection.clear();
      connection.send("create cache --file=" + getCliResource("dtemplate.xml").getPath() + " dtemplate");
      connection.send("create cache --template=dtemplate dcache");
      connection.send("cd caches/dcache");
      connection.assertContains("//containers/default/caches/dcache]>");
      connection.send("put k1 v1");
      connection.clear();
      connection.send("ls");
      connection.assertContains("k1");
      connection.send("get k1");
      connection.assertContains("v1");
      connection.clear();
      connection.send("get -x k1");
      connection.assertContains("cluster-primary-owner");
      connection.send("put --ttl=10 k2 v2");
      connection.clear();
      connection.send("describe k2");
      connection.assertContains("\"timetoliveseconds\" : [ \"10\" ]");

      connection.send("cd /containers/default/caches");

      connection.clear();
      connection.send("create cache xml '<distributed-cache/>'");
      connection.send("describe xml");
      connection.assertContains("\"mode\" : \"SYNC\"");

      connection.clear();
      connection.send("create cache json '{\"distributed-cache\":{}}'");
      connection.send("describe json");
      connection.assertContains("\"mode\" : \"SYNC\"");

      connection.clear();
      connection.send("create cache yaml 'distributedCache: ~'");
      connection.send("describe yaml");
      connection.assertContains("\"mode\" : \"SYNC\"");

      connection.send("schema upload -f=" + getCliResource("person.proto").getPath() + " person.proto");
      connection.send("create cache --file=" + getCliResource("qcache.xml").getPath() + " qcache");
      connection.clear();
      connection.send("cd /containers/default/schemas");
      connection.send("ls");
      connection.assertContains("person.proto");
      connection.send("cache qcache");
      connection.assertContains("//containers/default/caches/qcache]>");
      for (String person : Arrays.asList("jessicajones", "dannyrandy", "lukecage", "matthewmurdock")) {
         connection.send("put --encoding=application/json --file=" + getCliResource(person + ".json").getPath() + " " + person);
      }
      connection.clear();
      connection.send("ls");
      for (String person : Arrays.asList("jessicajones", "dannyrandy", "lukecage", "matthewmurdock")) {
         connection.assertContains(person);
      }
      connection.clear();
      connection.send("query \"from org.infinispan.rest.search.entity.Person p where p.gender = 'MALE'\"");
      connection.assertContains("\"hit_count\":3,");
      connection.clear();

      connection.send("index stats qcache");
      connection.assertContains("\"slowest\" : \"from org.infinispan.rest.search.entity.Person p where p.gender = 'MALE'\"");
      connection.clear();
      connection.send("index clear qcache");
      connection.send("index reindex qcache");
      connection.send("index reindex qcache --async");
      connection.send("index update-schema qcache --async");
      connection.send("index clear-stats qcache");
      connection.assertNotContains("Error");
      connection.clear();

      connection.send("stats");
      connection.assertContains("required_minimum_number_of_nodes");

      // COUNTERS
      connection.send("create counter --type=strong --storage=PERSISTENT --upper-bound=100 cnt1");
      connection.send("cd /containers/default/counters/cnt1");
      connection.send("describe");
      connection.assertContains("\"upper-bound\" : \"100\"");
      connection.clear();
      connection.send("add");
      connection.assertContains("1");
      connection.clear();
      connection.send("reset");
      connection.send("ls");
      connection.assertContains("0");
      connection.clear();
      connection.send("add --delta=100");
      connection.assertContains("100");

      // ALTER CACHE
      connection.send("create cache --file=" + getCliResource("xcache.xml").getPath() + " xcache");
      connection.send("describe /containers/default/caches/xcache");
      connection.assertContains("\"lifespan\" : \"60000\"");
      connection.assertContains("\"max-count\" : \"1000\"");
      connection.clear();
      connection.send("alter cache --file=" + getCliResource("xcache-alter.xml").getPath() + " xcache");
      connection.send("describe /containers/default/caches/xcache");
      connection.assertContains("\"lifespan\" : \"30000\"");
      connection.assertContains("\"max-count\" : \"2000\"");
      connection.clear();
      connection.send("alter cache xcache --attribute=memory.max-count --value=5000");
      connection.send("describe /containers/default/caches/xcache");
      connection.assertContains("\"lifespan\" : \"30000\"");
      connection.assertContains("\"max-count\" : \"5000\"");
      connection.clear();
   }

   @Test
   public void testCliBatch() {
      CliTestDriver cli = SERVERS.cli()
            .withProperty("serverAddress", SERVERS.getServerDriver().getServerAddress(0).getHostAddress())
            .withArguments("-f", getCliResource("batch.cli").getPath());
      CliConnection connection = cli.connection();
      connection.assertContains("Hi CLI running on " + System.getProperty("os.arch"));
      connection.assertContains("batch1");
   }

   @Test
   public void testCliBatchError() {
      CliTestDriver cli = SERVERS.cli()
            .withProperty("serverAddress", SERVERS.getServerDriver().getServerAddress(0).getHostAddress())
            .withArguments("-f", getCliResource("batch-error.cli").getPath());
      CliConnection connection = cli.connection();
      connection.assertContains("Hi CLI running on " + System.getProperty("os.arch"));
      connection.assertContains("batch-error.cli, line 2");
   }

   @Test
   public void testCliBatchPreconnect() {
      CliTestDriver cli = SERVERS.cli();
      cli.withArguments("-c", cli.url(), "-f", getCliResource("batch-preconnect.cli").getPath());
      CliConnection connection = cli.connection();
      connection.assertContains("Hi CLI");
      connection.assertContains("batch2");
   }

   @Test
   public void testCliTasks() {
      try (CliConnection terminal = SERVERS.cli().connect()) {
         terminal.send("cd tasks");
         terminal.send("ls");
         terminal.assertContains("@@cache@names");
         terminal.clear();
         terminal.send("task exec @@cache@names");
         terminal.assertContains("\"" + SCRIPT_CACHE_NAME + "\"");
         terminal.clear();
         File resource = getCliResource("hello.js");
         terminal.send("task upload --file=" + resource.getPath() + " hello");
         terminal.send("task exec hello -Pgreetee=world");
         terminal.assertContains("\"Hello world\"");
      }
   }

   @Test
   public void testCliCredentials() {
      CliConnection connection = SERVERS.cli().connection();
      String keyStore = Paths.get(System.getProperty("build.directory", ""), "key.store").toAbsolutePath().toString();
      connection.send("credentials add --path=" + keyStore + " --password=secret --credential=credential password");
      connection.send("credentials add --path=" + keyStore + " --password=secret --credential=credential another");
      connection.clear();
      connection.send("credentials ls --path=" + keyStore + " --password=secret");
      connection.assertContains("password");
      connection.assertContains("another");
   }

   @Test
   public void testCliAuthorization() {
      try (CliConnection connection = SERVERS.cli().connect()) {
         connection.send("user roles ls");
         connection.assertContains("\"admin\"");
         connection.send("user roles create --permissions=ALL_WRITE wizard");
         connection.send("user roles create --permissions=ALL_READ cleric");
         connection.clear();
         connection.send("user roles ls");
         connection.assertContains("\"wizard\"");
         connection.assertContains("\"cleric\"");
         connection.send("user roles grant --roles=wizard,cleric,admin admin");
         connection.clear();
         connection.send("user roles ls admin");
         connection.assertContains("\"wizard\"");
         connection.assertContains("\"cleric\"");
         connection.send("user roles deny --roles=cleric admin");
         connection.clear();
         connection.send("user roles ls admin");
         connection.assertContains("\"wizard\"");
         connection.assertNotContains("\"cleric\"");
         connection.send("user roles remove wizard");
         connection.clear();
         connection.send("user roles ls");
         connection.assertContains("\"cleric\"");
         connection.assertNotContains("\"wizard\"");
      }
   }

   @Test
   public void testCliUploadProtobufSchemas() {
      try (CliConnection connection = SERVERS.cli().connect()) {
         // upload
         connection.send("schema upload --file=" + getCliResource("person.proto").getPath() + " person.proto");
         connection.assertContains("\"error\" : null");
         connection.clear();
         connection.send("cd /containers/default/schemas");
         connection.send("ls");
         connection.assertContains("person.proto");
         connection.clear();
         connection.send("schema ls");
         connection.assertContains("person.proto");
         connection.send("schema get person.proto");
         connection.assertContains("PhoneNumber");
         connection.send("schema remove person.proto");
         connection.clear();
         connection.send("schema ls");
         connection.assertContains("[]");
      }
   }

   @Test
   public void testCliHttpBenchmark() {
      CliTestDriver cli = SERVERS.cli();
      CliConnection connection = cli.connection();
      // no cache
      connection.send("benchmark " + cli.url());
      connection.assertContains("IllegalArgumentException: Could not find cache");
   }

   @Test
   public void testCliConfigPersistence() {
      CliTestDriver cli = SERVERS.cli();
      try (CliConnection connection = cli.connection()) {
         connection.send("config set autoconnect-url " + cli.url());
         connection.clear();
         connection.send("config get autoconnect-url");
         connection.assertContains(cli.url());
      }

      // Close and recreate the CLI so that auto-connection kicks in
      try (CliConnection connection = SERVERS.cli().connection()) {
         connection.assertContains("//containers/default]>");
         connection.send("config set autoconnect-url");
      }
   }

   @Test
   public void testCliCacheAvailability() {
      CliConnection connection = SERVERS.cli().connect();
      var cacheName = "qcache";
      connection.send("create cache --file=" + getCliResource("qcache.xml").getPath() + " " + cacheName);
      connection.send("availability " + cacheName);
      connection.assertContains("AVAILABLE");
      connection.send("availability --mode=DEGRADED_MODE " + cacheName);
      connection.send("availability " + cacheName);
      connection.assertContains("DEGRADED_MODE");
      connection.send("availability --mode=AVAILABILITY " + cacheName);
      connection.send("availability " + cacheName);
      connection.assertContains("AVAILABLE");
   }

   @Test
   public void testCliAlternateContext() {
      CliTestDriver cli = SERVERS.cli().withPort(11225);
      CliConnection connection = cli.connection();
      connection.send("connect --context-path=/relax " + cli.url());
      connection.assertContains("//containers/default]>");
      connection.clear();
   }

   private File getCliResource(String resource) {
      Path dest = SERVERS.getServerDriver().getRootDir().toPath().resolve(resource);
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
