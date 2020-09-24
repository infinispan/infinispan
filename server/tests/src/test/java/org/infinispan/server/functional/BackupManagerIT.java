package org.infinispan.server.functional;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestCacheManagerClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestClusterClient;
import org.infinispan.client.rest.RestCounterClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestTaskClient;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.configuration.Element;
import org.infinispan.test.TestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Ryan Emerson
 * @since 12.0
 */
public class BackupManagerIT extends AbstractMultiClusterIT {

   static final File WORKING_DIR = new File(CommonsTestingUtil.tmpDirectory(BackupManagerIT.class));
   static final int NUM_ENTRIES = 10;

   public BackupManagerIT() {
      super("configuration/ClusteredServerTest.xml");
   }

   @BeforeClass
   public static void setup() {
      WORKING_DIR.mkdirs();
   }

   @AfterClass
   public static void teardown() {
      Util.recursiveFileRemove(WORKING_DIR);
   }

   @Test
   public void testManagerBackupUpload() throws Exception {
      String name = "testManagerBackup";
      performTest(
            client -> {
               RestCacheManagerClient cm = client.cacheManager("clustered");
               RestResponse response = await(cm.createBackup(name));
               assertEquals(202, response.getStatus());
               return awaitOk(() -> cm.getBackup(name, false));
            },
            client -> await(client.cacheManager("clustered").deleteBackup(name)),
            (zip, client) -> {
               RestCacheManagerClient cm = client.cacheManager("clustered");
               RestResponse response = await(cm.restore(name, zip));
               assertEquals(202, response.getStatus());
               return awaitCreated(() -> cm.getRestore(name));
            },
            this::assertWildcardContent
      );
   }

   @Test
   public void testManagerBackupFromFile() throws Exception {
      String name = "testManagerBackup";
      performTest(
            client -> {
               RestCacheManagerClient cm = client.cacheManager("clustered");
               RestResponse response = await(cm.createBackup(name));
               assertEquals(202, response.getStatus());
               response.close();
               return awaitOk(() -> cm.getBackup(name, false));
            },
            client -> await(client.cacheManager("clustered").deleteBackup(name)),
            (zip, client) -> {
               RestCacheManagerClient cm = client.cacheManager("clustered");
               RestResponse response = await(cm.restore(name, zip.getPath(), null));
               assertEquals(202, response.getStatus());
               return awaitCreated(() -> cm.getRestore(name));
            },
            this::assertWildcardContent
      );
   }

   @Test
   public void testManagerBackupParameters() throws Exception {
      String name = "testManagerBackupParameters";
      performTest(
            client -> {
               Map<String, List<String>> params = new HashMap<>();
               params.put("caches", Collections.singletonList("*"));
               params.put("counters", Collections.singletonList("weak-volatile"));

               RestCacheManagerClient cm = client.cacheManager("clustered");
               RestResponse response = await(cm.createBackup(name, params));
               assertEquals(202, response.getStatus());
               return awaitOk(() -> cm.getBackup(name, false));
            },
            client -> await(client.cacheManager("clustered").deleteBackup(name)),
            (zip, client) -> {
               Map<String, List<String>> params = new HashMap<>();
               params.put("caches", Collections.singletonList("cache1"));
               params.put("counters", Collections.singletonList("*"));

               RestCacheManagerClient cm = client.cacheManager("clustered");
               RestResponse response = await(cm.restore(name, zip, params));
               assertEquals(202, response.getStatus());
               return awaitCreated(() -> cm.getRestore(name));
            },
            client -> {
               // Assert that only caches and the specified "weak-volatile" counter have been backed up. Internal caches will still be present
               assertEquals("[\"___protobuf_metadata\",\"memcachedCache\",\"cache1\",\"___script_cache\"]", await(client.caches()).getBody());
               assertEquals("[\"weak-volatile\"]", await(client.counters()).getBody());
               assertEquals(404, await(client.schemas().get("schema.proto")).getStatus());
               assertEquals("[]", await(client.tasks().list(RestTaskClient.ResultType.USER)).getBody());
            }
      );
   }

   @Test
   public void testCreateDuplicateBackupResources() throws Exception {
      String backupName = "testCreateDuplicateBackupResources";
      // Start the source cluster
      startSourceCluster();
      RestClient client = source.getClient();

      populateContainer(client);

      RestCacheManagerClient cm = client.cacheManager("clustered");
      RestResponse response = await(cm.createBackup(backupName));
      assertEquals(202, response.getStatus());

      response = await(cm.createBackup(backupName));
      assertEquals(409, response.getStatus());

      response = await(cm.deleteBackup(backupName));
      // Expect a 202 response as the previous backup will be in progress, so the request is accepted for now
      assertEquals(202, response.getStatus());

      // Now wait until the backup has actually been deleted so that we successfully create another with the same name
      awaitStatus(() -> cm.deleteBackup(backupName), 202, 404);

      response = await(cm.createBackup(backupName));
      assertEquals(202, response.getStatus());

      // Wait for the backup operation to finish
      awaitOk(() -> cm.getBackup(backupName, false));
      response = await(cm.deleteBackup(backupName));
      assertEquals(204, response.getStatus());
   }

   @Test
   public void testManagerRestoreParameters() throws Exception {
      String name = "testManagerRestoreParameters";
      performTest(
            client -> {
               // Create a backup of all container content
               RestCacheManagerClient cm = client.cacheManager("clustered");
               RestResponse response = await(cm.createBackup(name));
               assertEquals(202, response.getStatus());
               return awaitOk(() -> cm.getBackup(name, false));
            },
            client -> await(client.cacheManager("clustered").deleteBackup(name)),
            (zip, client) -> {
               // Request that only the 'test.js' script is restored
               Map<String, List<String>> params = new HashMap<>();
               params.put("scripts", Collections.singletonList("test.js"));
               RestCacheManagerClient cm = client.cacheManager("clustered");
               RestResponse response = await(cm.restore(name, zip, params));
               assertEquals(202, response.getStatus());
               return awaitCreated(() -> cm.getRestore(name));
            },
            client -> {
               // Assert that the test.js script has been restored
               List<Json> scripts = Json.read(await(client.tasks().list(RestTaskClient.ResultType.USER)).getBody()).asJsonList();
               assertEquals(1, scripts.size());
               assertEquals("test.js", scripts.iterator().next().at("name").asString());

               // Assert that no other content has been restored
               assertEquals("[\"___protobuf_metadata\",\"memcachedCache\",\"___script_cache\"]", await(client.caches()).getBody());
               assertEquals("[]", await(client.counters()).getBody());
               assertEquals(404, await(client.schemas().get("schema.proto")).getStatus());
            }
      );
   }

   @Test
   public void testClusterBackupUpload() throws Exception {
      String name = "testClusterBackup";
      performTest(
            client -> {
               RestClusterClient cluster = client.cluster();
               RestResponse response = await(cluster.createBackup(name));
               assertEquals(202, response.getStatus());
               return awaitOk(() -> cluster.getBackup(name, false));
            },
            client -> await(client.cacheManager("clustered").deleteBackup(name)),
            (zip, client) -> {
               RestClusterClient c = client.cluster();
               RestResponse response = await(c.restore(name, zip));
               assertEquals(202, response.getStatus());
               return awaitCreated(() -> c.getRestore(name));
            },
            this::assertWildcardContent
      );
   }

   @Test
   public void testClusterBackupFromFile() throws Exception {
      String name = "testClusterBackup";
      performTest(
            client -> {
               RestClusterClient cluster = client.cluster();
               RestResponse response = await(cluster.createBackup(name));
               assertEquals(202, response.getStatus());
               return awaitOk(() -> cluster.getBackup(name, false));
            },
            client -> await(client.cacheManager("clustered").deleteBackup(name)),
            (zip, client) -> {
               RestClusterClient c = client.cluster();
               RestResponse response = await(c.restore(name, zip.getPath()));
               assertEquals(202, response.getStatus());
               return awaitCreated(() -> c.getRestore(name));
            },
            this::assertWildcardContent
      );
   }

   private RestResponse awaitOk(Supplier<CompletionStage<RestResponse>> request) {
      return awaitStatus(request, 202, 200);
   }

   private RestResponse awaitCreated(Supplier<CompletionStage<RestResponse>> request) {
      return awaitStatus(request, 202, 201);
   }

   private RestResponse awaitStatus(Supplier<CompletionStage<RestResponse>> request, int pendingStatus, int completeStatus) {
      int count = 0;
      RestResponse response;
      while ((response = await(request.get())).getStatus() == pendingStatus || count++ < 100) {
         TestingUtil.sleepThread(10);
         response.close();
      }
      assertEquals(completeStatus, response.getStatus());
      return response;
   }

   private void performTest(Function<RestClient, RestResponse> backupAndDownload,
                            Function<RestClient, RestResponse> delete,
                            BiFunction<File, RestClient, RestResponse> restore,
                            Consumer<RestClient> assertTargetContent) throws Exception {
      // Start the source cluster
      startSourceCluster();
      RestClient client = source.getClient();

      // Populate the source container
      populateContainer(client);

      // Perform the backup and download
      RestResponse getResponse = backupAndDownload.apply(client);
      String fileName = getResponse.getHeader("Content-Disposition").split("=")[1];

      // Delete the backup from the server
      RestResponse deleteResponse = delete.apply(client);
      assertEquals(204, deleteResponse.getStatus());
      deleteResponse.close();

      // Ensure that all of the backup files have been deleted from the source cluster
      // We must wait for a short period time here to ensure that the returned entity has actually been removed from the filesystem
      Thread.sleep(50);
      assertNoServerBackupFilesExist(source);

      // Shutdown the source cluster
      stopSourceCluster();

      // Start the target cluster
      startTargetCluster();
      client = target.getClient();

      // Copy the returned zip bytes to the local working dir
      File backupZip = new File(WORKING_DIR, fileName);
      try (InputStream is = getResponse.getBodyAsStream()) {
         Files.copy(is, backupZip.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
      getResponse.close();

      // Upload the backup to the target cluster
      RestResponse restoreResponse = restore.apply(backupZip, client);
      assertEquals(restoreResponse.getBody(), 201, restoreResponse.getStatus());
      restoreResponse.close();

      // Assert that all content has been restored as expected
      assertTargetContent.accept(client);

      // Ensure that the backup files have been deleted from the target cluster
      assertNoServerBackupFilesExist(target);
      stopTargetCluster();
   }

   private void populateContainer(RestClient client) throws Exception {
      String cacheName = "cache1";
      createCache(cacheName, new ConfigurationBuilder(), client);

      RestCacheClient cache = client.cache(cacheName);
      for (int i = 0; i < NUM_ENTRIES; i++) {
         join(cache.put(String.valueOf(i), String.valueOf(i)));
      }
      assertEquals(NUM_ENTRIES, getCacheSize(cacheName, client));

      createCounter("weak-volatile", Element.WEAK_COUNTER, Storage.VOLATILE, client, 0);
      createCounter("weak-persistent", Element.WEAK_COUNTER, Storage.PERSISTENT, client, -100);
      createCounter("strong-volatile", Element.STRONG_COUNTER, Storage.VOLATILE, client, 50);
      createCounter("strong-persistent", Element.STRONG_COUNTER, Storage.PERSISTENT, client, 0);

      addSchema(client);

      try (InputStream is = BackupManagerIT.class.getResourceAsStream("/scripts/test.js")) {
         String script = CommonsTestingUtil.loadFileAsString(is);
         RestResponse rsp = await(client.tasks().uploadScript("test.js", RestEntity.create(MediaType.APPLICATION_JAVASCRIPT, script)));
         assertEquals(200, rsp.getStatus());
      }
   }

   private void assertWildcardContent(RestClient client) {
      String cacheName = "cache1";
      assertEquals(Integer.toString(NUM_ENTRIES), await(client.cache(cacheName).size()).getBody());

      assertCounter(client, "weak-volatile", Element.WEAK_COUNTER, Storage.VOLATILE, 0);
      assertCounter(client, "weak-persistent", Element.WEAK_COUNTER, Storage.PERSISTENT, -100);
      assertCounter(client, "strong-volatile", Element.STRONG_COUNTER, Storage.VOLATILE, 50);
      assertCounter(client, "strong-persistent", Element.STRONG_COUNTER, Storage.PERSISTENT, 0);

      RestResponse rsp = await(client.schemas().get("schema.proto"));
      assertEquals(200, rsp.getStatus());
      assertTrue(rsp.getBody().contains("message Person"));

      rsp = await(client.tasks().list(RestTaskClient.ResultType.USER));
      assertEquals(200, rsp.getStatus());

      Json json = Json.read(rsp.getBody());
      assertTrue(json.isArray());
      List<Json> tasks = json.asJsonList();
      assertEquals(1, tasks.size());
      assertEquals("test.js", tasks.get(0).at("name").asString());
   }

   private void createCounter(String name, Element type, Storage storage, RestClient client, long delta) {
      String config = String.format("{\n" +
            "    \"%s\":{\n" +
            "        \"initial-value\":0,\n" +
            "        \"storage\":\"%s\"\n" +
            "    }\n" +
            "}", type, storage.toString());
      RestCounterClient counterClient = client.counter(name);
      RestResponse rsp = await(counterClient.create(RestEntity.create(MediaType.APPLICATION_JSON, config)));
      assertEquals(200, rsp.getStatus());

      if (delta != 0) {
         rsp = await(counterClient.add(delta));
         assertEquals(name, name.contains("strong") ? 200 : 204, rsp.getStatus());
         assertNotNull(rsp.getBody());
      }
   }

   private void assertCounter(RestClient client, String name, Element type, Storage storage, long expectedValue) {
      RestResponse rsp = await(client.counter(name).configuration());
      assertEquals(200, rsp.getStatus());
      String content = rsp.getBody();
      Json config = Json.read(content).at(type.toString());
      assertEquals(name, config.at("name").asString());
      assertEquals(storage.toString(), config.at("storage").asString());
      assertEquals(0, config.at("initial-value").asInteger());

      rsp = await(client.counter(name).get());
      assertEquals(200, rsp.getStatus());
      assertEquals(expectedValue, Long.parseLong(rsp.getBody()));
   }

   private void assertNoServerBackupFilesExist(Cluster cluster) {
      for (int i = 0; i < 2; i++) {
         Path root = cluster.driver.getRootDir().toPath();
         File workingDir = root.resolve(Integer.toString(i)).resolve("data").resolve("backup-manager").toFile();
         assertTrue(workingDir.isDirectory());
         String[] files = workingDir.list();
         assertNotNull(files);
         assertEquals(Arrays.toString(files), 0, files.length);
      }
   }
}
