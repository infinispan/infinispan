package org.infinispan.server.core.backup;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN_TYPE;
import static org.infinispan.functional.FunctionalTestUtils.MAX_WAIT_SECS;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.server.core.BackupManager.Resources.Type.CACHES;
import static org.infinispan.server.core.BackupManager.Resources.Type.CACHE_CONFIGURATIONS;
import static org.infinispan.server.core.backup.Constants.CONTAINERS_PROPERTIES_FILE;
import static org.infinispan.server.core.backup.Constants.CONTAINER_KEY;
import static org.infinispan.server.core.backup.Constants.GLOBAL_CONFIG_FILE;
import static org.infinispan.server.core.backup.Constants.MANIFEST_PROPERTIES_FILE;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.EncodingConfigurationBuilder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.BackupManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.function.TriConsumer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Ryan Emerson
 * @since 12.0
 */
@Test(groups = "functional", testName = "server.core.BackupManagerImplTest")
public class BackupManagerImplTest extends AbstractInfinispanTest {

   private static File workingDir;

   @BeforeMethod
   void setup() {
      workingDir = new File(CommonsTestingUtil.tmpDirectory(BackupManagerImplTest.class));
      Util.recursiveFileRemove(workingDir);
      workingDir.mkdirs();
   }

   @AfterMethod
   static void teardown() {
      Util.recursiveFileRemove(workingDir);
   }

   public void testMissingCacheOnBackup() {
      invalidWriteTest("'example-cache' does not exist", new BackupManagerResources.Builder()
            .addCaches("example-cache")
            .build()
      );
   }

   public void testMissingCacheConfigOnBackup() {
      invalidWriteTest("'template' does not exist", new BackupManagerResources.Builder()
            .addCacheConfigurations("template")
            .build()
      );
   }

   private void invalidWriteTest(String msg, BackupManager.Resources params) {
      withBackupManager((cm, backupManager) -> {
         try {
            CompletionStage<Path> stage = backupManager.create("invalidWriteTest", null, Collections.singletonMap("default", params));
            stage.toCompletableFuture().get(MAX_WAIT_SECS, TimeUnit.SECONDS);
            fail();
         } catch (TimeoutException | InterruptedException e) {
            fail();
         } catch (ExecutionException e) {
            Throwable t = e.getCause();
            assertTrue(t instanceof CacheException);
            assertTrue(t.getMessage().contains("Cannot create cluster backup"));
            t = t.getCause();
            assertTrue(t instanceof CompletionException);
            t = t.getCause();
            assertTrue(t instanceof CacheException);
            assertTrue(t.getMessage().contains(msg));
         }
         return CompletableFutures.completedNull();
      });
   }

   public void testMissingCacheOnRestore() throws Exception {
      String resource = "example-cache";
      invalidRestoreTest(resource, new BackupManagerResources.Builder().addCaches(resource).build());
   }

   public void testMissingCacheConfigOnRestore() throws Exception {
      String resource = "template";
      invalidRestoreTest(resource, new BackupManagerResources.Builder().addCacheConfigurations(resource).build());
   }

   private void invalidRestoreTest(String resourceName, BackupManager.Resources params) throws Exception {
      String name = "invalidRestoreTest";
      createAndRestore(
            (source, backupManager) -> backupManager.create(name, null),
            (target, backupManager, backup) -> {
               assertTrue(target.getCacheNames().isEmpty());
               assertNull(target.getCacheConfiguration("cache-config"));
               Map<String, BackupManager.Resources> paramMap = Collections.singletonMap("default", params);
               CompletableFuture<Void> stage = backupManager.restore(name, backup, paramMap).toCompletableFuture();

               try {
                  stage.get(MAX_WAIT_SECS, TimeUnit.SECONDS);
               } catch (ExecutionException e) {
                  assertTrue(stage.isCompletedExceptionally());
                  Throwable t = e.getCause();
                  assertTrue(t instanceof CacheException);
                  assertTrue(t.getMessage().contains("Cannot restore cluster backup"));
                  t = t.getCause();
                  assertTrue(t instanceof CompletionException);
                  t = t.getCause();
                  assertTrue(t instanceof CacheException);
                  assertTrue(t.getMessage().contains(String.format("'[%s]' not found in the backup archive", resourceName)));
               } catch (Exception e) {
                  fail();
               }
            }
      );
   }

   public void testBackupAndRestoreIgnoreResources() throws Exception {
      String name = "testBackupAndRestoreIgnoreResources";
      createAndRestore(
            (source, backupManager) -> {
               source.defineConfiguration("cache-config", config(APPLICATION_OBJECT_TYPE, true));
               Cache<String, String> cache = source.createCache("cache", config(APPLICATION_OBJECT_TYPE));
               cache.put("key", "value");
               return backupManager.create(name, null);
            },
            (target, backupManager, backup) -> {
               assertTrue(target.getCacheNames().isEmpty());
               assertNull(target.getCacheConfiguration("cache-config"));
               Map<String, BackupManager.Resources> paramMap = Collections.singletonMap("default",
                     new BackupManagerResources.Builder()
                           .includeAll()
                           .ignore(CACHES)
                           .build()
               );
               await(backupManager.restore(name, backup, paramMap));
               assertTrue(target.getCacheNames().isEmpty());
               assertNotNull(target.getCacheConfiguration("cache-config"));
            });
   }

   public void testBackupAndRestoreWildcardResources() throws Exception {
      String name = "testBackupAndRestoreWildcardResources";
      createAndRestore(
            (source, backupManager) -> {
               source.defineConfiguration("cache-config", config(APPLICATION_OBJECT_TYPE, true));
               Cache<String, String> cache = source.createCache("cache", config(APPLICATION_OBJECT_TYPE));
               cache.put("key", "value");
               return backupManager.create(name, null);
            },
            (target, backupManager, backup) -> {
               assertTrue(target.getCacheNames().isEmpty());
               assertNull(target.getCacheConfiguration("cache-config"));
               await(backupManager.restore(name, backup));
               assertFalse(target.getCacheNames().isEmpty());
               assertNotNull(target.getCacheConfiguration("cache-config"));

               Cache<String, String> cache = target.getCache("cache");
               assertNotNull(cache);
               assertEquals("value", cache.get("key"));
            });
   }

   public void testCustomWorkingDirectory() throws IOException {
      String backupName = "customDir";
      Path customDir = new File(workingDir, "custom-dir").toPath();
      Files.createDirectory(customDir);
      Path zip = withBackupManager((cm, bm) -> bm.create(backupName, customDir));
      assertEquals(customDir.resolve(backupName).resolve(backupName + ".zip"), zip);
   }

   private void createAndRestore(BiFunction<DefaultCacheManager, BackupManager, CompletionStage<Path>> backup,
                                 TriConsumer<DefaultCacheManager, BackupManager, Path> restore) {
      Path zip = withBackupManager(backup);
      withBackupManager((cm, bm) -> {
         restore.accept(cm, bm, zip);
         return CompletableFutures.completedNull();
      });
   }

   private <T> T withBackupManager(BiFunction<DefaultCacheManager, BackupManager, CompletionStage<T>> function) {
      try (DefaultCacheManager cm = new DefaultCacheManager()) {
         BlockingManager blockingManager = cm.getGlobalComponentRegistry().getComponent(BlockingManager.class);
         BackupManager backupManager = new BackupManagerImpl(blockingManager, cm, Collections.singletonMap("default", cm), workingDir.toPath());
         backupManager.init();
         return await(function.apply(cm, backupManager));
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   public void testBackupAndRestoreMultipleContainers() throws Exception {
      int numEntries = 100;

      String container1 = "container1";
      String container2 = "container2";
      Map<String, DefaultCacheManager> readerManagers = createManagerMap(container1, container2);
      Map<String, DefaultCacheManager> writerManagers = createManagerMap(container1, container2);
      try {
         DefaultCacheManager sourceManager1 = writerManagers.get(container1);
         DefaultCacheManager sourceManager2 = writerManagers.get(container2);

         sourceManager1.defineConfiguration("example-template", config(APPLICATION_OBJECT_TYPE, true));
         sourceManager1.defineConfiguration("object-cache", config(APPLICATION_OBJECT_TYPE));
         sourceManager1.defineConfiguration("protostream-cache", config(APPLICATION_PROTOSTREAM_TYPE));
         sourceManager1.defineConfiguration("empty-cache", config(APPLICATION_UNKNOWN_TYPE));
         sourceManager2.defineConfiguration("container2-cache", config(APPLICATION_UNKNOWN_TYPE));

         IntStream.range(0, numEntries).forEach(i -> sourceManager1.getCache("object-cache").put(i, i));
         IntStream.range(0, numEntries).forEach(i -> sourceManager1.getCache("protostream-cache").put(i, i));

         ParserRegistry parserRegistry = new ParserRegistry();
         BlockingManager blockingManager = writerManagers.values().iterator().next().getGlobalComponentRegistry().getComponent(BlockingManager.class);
         String name = "testBackupAndRestoreMultipleContainers";
         BackupWriter writer = new BackupWriter(name, blockingManager, writerManagers, parserRegistry, workingDir.toPath());

         Map<String, BackupManager.Resources> paramMap = new HashMap<>(2);
         paramMap.put(container1,
               new BackupManagerResources.Builder()
                     .addCaches("object-cache", "protostream-cache", "empty-cache")
                     .addCacheConfigurations("example-template")
                     .build()
         );

         paramMap.put(container2,
               new BackupManagerResources.Builder()
                     .addCaches("container2-cache")
                     .build()
         );

         Path backupZip = await(writer.create(paramMap));
         assertNotNull(backupZip);

         Path extractedRoot = workingDir.toPath().resolve("extracted");
         File extractedDir = extractedRoot.toFile();
         extractedDir.mkdir();
         extractBackup(backupZip.toFile(), extractedDir);

         assertFileExists(extractedRoot, MANIFEST_PROPERTIES_FILE);

         Path containerPath = path(extractedRoot, CONTAINER_KEY, container1);
         assertFileExists(containerPath, CONTAINERS_PROPERTIES_FILE);
         assertFileExists(containerPath, GLOBAL_CONFIG_FILE);
         assertFileExists(containerPath, CACHE_CONFIGURATIONS.toString(), "example-template.xml");
         assertFileExists(containerPath, CACHES.toString(), "object-cache", "object-cache.xml");
         assertFileExists(containerPath, CACHES.toString(), "object-cache", "object-cache.dat");
         assertFileExists(containerPath, CACHES.toString(), "protostream-cache", "protostream-cache.xml");
         assertFileExists(containerPath, CACHES.toString(), "protostream-cache", "protostream-cache.dat");
         assertFileExists(containerPath, CACHES.toString(), "empty-cache", "empty-cache.xml");
         assertFileExists(containerPath, CACHES.toString(), "empty-cache", "empty-cache.dat");

         containerPath = path(extractedRoot, CONTAINER_KEY, container2);
         assertFileExists(containerPath, GLOBAL_CONFIG_FILE);
         assertFileExists(containerPath, CONTAINERS_PROPERTIES_FILE);
         assertFileExists(containerPath, CACHES.toString(), "container2-cache", "container2-cache.xml");
         assertFileDoesNotExist(containerPath, CACHES.toString(), "object-cache");
         assertFileDoesNotExist(containerPath, CACHES.toString(), "protostream-cache");


         BackupReader reader = new BackupReader(blockingManager, readerManagers, parserRegistry);
         CompletionStage<Void> restoreStage = reader.restore(backupZip, paramMap);
         await(restoreStage);

         DefaultCacheManager targetManager1 = readerManagers.get(container1);
         DefaultCacheManager targetManager2 = readerManagers.get(container2);


         Configuration template = targetManager1.getCacheConfiguration("example-template");
         assertNotNull(template);
         assertTrue(template.isTemplate());

         Cache<Integer, Integer> objectCache = targetManager1.getCache("object-cache");
         assertFalse(objectCache.isEmpty());
         assertEquals(100, objectCache.size());
         assertEquals(Integer.valueOf(50), objectCache.get(50));

         Cache<Integer, Integer> protoCache = targetManager1.getCache("protostream-cache");
         assertFalse(protoCache.isEmpty());
         assertEquals(100, protoCache.size());
         assertEquals(Integer.valueOf(1), protoCache.get(1));

         Cache<Object, Object> emptyCache = targetManager1.getCache("empty-cache");
         assertTrue(emptyCache.isEmpty());

         Cache<Object, Object> container2Cache = targetManager2.getCache("container2-cache");
         assertTrue(container2Cache.isEmpty());
      } finally {
         writerManagers.values().forEach(EmbeddedCacheManager::stop);
         readerManagers.values().forEach(EmbeddedCacheManager::stop);
      }
   }

   private Map<String, DefaultCacheManager> createManagerMap(String... containers) {
      return Arrays.stream(containers)
            .collect(Collectors.toMap(Function.identity(), name -> new DefaultCacheManager()));
   }

   private Configuration config(String type) {
      return config(type, false);
   }

   private Configuration config(String type, boolean template) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      EncodingConfigurationBuilder encoding = builder.encoding();
      encoding.key().mediaType(type);
      encoding.value().mediaType(type);
      builder.template(template);
      return builder.build();
   }

   private Path path(Path root, String... paths) {
      return Paths.get(root.toString(), paths);
   }

   private void assertFileExists(Path root, String... paths) {
      Path path = path(root, paths);
      assertTrue(path.toFile().exists());
   }

   private void assertFileDoesNotExist(Path root, String... paths) {
      Path path = path(root, paths);
      assertFalse(path.toFile().exists());
   }

   private void extractBackup(File backup, File destDir) throws IOException {
      try (ZipFile zip = new ZipFile(backup)) {
         Enumeration<? extends ZipEntry> zipEntries = zip.entries();
         while (zipEntries.hasMoreElements()) {
            ZipEntry entry = zipEntries.nextElement();
            File file = new File(destDir, entry.getName());
            if (entry.isDirectory()) {
               file.mkdirs();
            } else {
               file.getParentFile().mkdirs();
               Files.copy(zip.getInputStream(entry), file.toPath());
            }
         }
      }
   }
}
