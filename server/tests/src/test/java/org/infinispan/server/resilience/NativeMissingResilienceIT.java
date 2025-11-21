package org.infinispan.server.resilience;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.util.Util.recursiveFileRemove;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.server.test.core.tags.Resilience;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ArchivePath;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.importer.ZipImporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import party.iroiro.luajava.lua51.Lua51;

@Resilience
public class NativeMissingResilienceIT {

   private static final String OTHER_SERVER_PATH;
   static {
      String directory = System.getProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_DIR);
      Path source = Paths.get(directory);
      Path target = source.resolve("..").resolve("infinispan-server-copied");
      try {
         target.toFile().mkdir();
         Util.recursiveDirectoryCopy(source, target);
      } catch (IOException e) {
         throw new AssertionError("Could not copy directories", e);
      }

      OTHER_SERVER_PATH = target.toAbsolutePath().toString();
   }

   @RegisterExtension
   public static DelayedServerStartExtension EXTENSION = new DelayedServerStartExtension(
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(1)
               .runMode(ServerRunMode.CONTAINER)
               .artifacts(NativeMissingResilienceIT.createMangledLuaJar())
               .property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_DIR, OTHER_SERVER_PATH)
               .build()
   );

   @Test
   public void testUnresponsiveNode() throws Exception {
      Class.forName(Lua51.class.getName());
      try {
         EXTENSION.startServers();

         verifyCacheWorkingHotRod();
         verifyCacheWorkingResp();
      } finally {
         recursiveFileRemove(OTHER_SERVER_PATH);
         EXTENSION.stopServers();
      }
   }

   private void verifyCacheWorkingHotRod() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.socketTimeout(1000).connectionTimeout(1000).maxRetries(10).connectionPool().maxActive(1);
      RemoteCache<String, String> cache = EXTENSION.servers.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.REPL_SYNC).create();

      cache.put("k1", "v1");
      assertThat(cache.get("k1")).isEqualTo("v1");
   }

   private void verifyCacheWorkingResp() {
      RedisClient client = RedisClient.create(EXTENSION.servers.resp().connectionString(0));
      try (StatefulRedisConnection<String, String> conn = client.connect()) {
         RedisCommands<String, String> sync = conn.sync();
         sync.set("k1", "v1");
         assertThat(sync.get("k1")).isEqualTo("v1");
      } finally {
         client.shutdown();
      }
   }

   public static class DelayedServerStartExtension implements BeforeAllCallback, BeforeEachCallback {

      private ExtensionContext beforeAllContext;
      private ExtensionContext beforeEachContext;
      private final InfinispanServerExtension servers;

      public DelayedServerStartExtension(InfinispanServerExtension servers) {
         this.servers = servers;
      }

      @Override
      public void beforeAll(ExtensionContext context) {
         this.beforeAllContext = context;
      }

      public void startServers() throws Exception {
         servers.beforeAll(beforeAllContext);
         servers.beforeEach(beforeEachContext);
      }

      public void stopServers() {
         servers.afterEach(beforeEachContext);
         servers.afterAll(beforeAllContext);
      }

      @Override
      public void beforeEach(ExtensionContext context) {
         beforeEachContext = context;
      }
   }

   private static Archive<?> createMangledLuaJar() {
      // Create JAR from original server path.
      String directory = System.getProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_DIR);
      Path root = Paths.get(directory);
      Path lib = root.resolve("lib");
      try (Stream<Path> jars = Files.list(lib)) {
         Optional<Path> jar = jars
               .filter(Files::isRegularFile)
               .filter(p -> p.getFileName().toString().startsWith("infinispan-lua51-platform-"))
               .filter(p -> p.getFileName().toString().endsWith(".jar"))
               .findFirst();
         return jar.map(NativeMissingResilienceIT::createMangledLuaJar).orElse(null);
      } catch (IOException e) {
         throw new AssertionError("Could not create corrupted jar", e);
      } finally {
         // Remove the JAR from the copied server folder.
         // The original must be kept.
         destroyLuaJar();
      }
   }

   private static void destroyLuaJar() {
      Path lib = Path.of(OTHER_SERVER_PATH, "lib");
      try (Stream<Path> jars = Files.list(lib)) {
         Optional<Path> jar = jars
               .filter(Files::isRegularFile)
               .filter(p -> p.getFileName().toString().startsWith("infinispan-lua51-platform-"))
               .filter(p -> p.getFileName().toString().endsWith(".jar"))
               .findFirst();
         if (jar.isEmpty())
            return;

         Path jarPath = jar.get();
         if (!jarPath.toFile().delete()) {
            System.out.println("Failed to delete jar: " + jarPath);
         }
      } catch (IOException e) {
         throw new AssertionError("Could not destroy jar", e);
      }
   }

   private static Archive<?> createMangledLuaJar(Path originalJar) {
      Archive<?> archive = ShrinkWrap.create(ZipImporter.class, originalJar.getFileName().toString())
            .importFrom(originalJar.toFile())
            .as(JavaArchive.class);

      List<ArchivePath> binaries = archive.getContent().keySet().stream()
            .filter(p -> p.get().endsWith(".so") || p.get().endsWith(".dll"))
            .toList();

      for (ArchivePath binary : binaries) {
         archive.delete(binary);
      }

      return archive;
   }
}
