package org.infinispan.server.functional;

import static java.lang.String.format;
import static org.infinispan.client.rest.RestResponse.ACCEPTED;
import static org.infinispan.client.rest.RestResponse.CONFLICT;
import static org.infinispan.client.rest.RestResponse.CREATED;
import static org.infinispan.client.rest.RestResponse.NOT_FOUND;
import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.commons.internal.InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME;
import static org.infinispan.commons.internal.InternalCacheNames.SCRIPT_CACHE_NAME;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.server.core.BackupManager.Resources.Type.CACHES;
import static org.infinispan.server.core.BackupManager.Resources.Type.COUNTERS;
import static org.infinispan.server.core.BackupManager.Resources.Type.PROTO_SCHEMAS;
import static org.infinispan.server.core.BackupManager.Resources.Type.TASKS;
import static org.infinispan.server.core.BackupManager.Resources.Type.TEMPLATES;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.assertStatusAndBodyContains;
import static org.infinispan.server.test.core.Common.assertStatusAndBodyEquals;
import static org.infinispan.server.test.core.Common.awaitResponse;
import static org.infinispan.server.test.core.Common.awaitStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.zip.ZipFile;

import org.infinispan.cli.commands.CLI;
import org.infinispan.cli.impl.AeshDelegatingShell;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestContainerClient;
import org.infinispan.client.rest.RestCounterClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestTaskClient;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.configuration.Element;
import org.infinispan.server.core.BackupManager;
import org.infinispan.server.test.core.AeshTestConnection;
import org.infinispan.server.test.core.Common;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * @author Ryan Emerson
 * @since 12.0
 */
public class BackupManagerIT extends AbstractMultiClusterIT {

   static final File WORKING_DIR = new File(CommonsTestingUtil.tmpDirectory(BackupManagerIT.class));
   static final int NUM_ENTRIES = 10;

   public BackupManagerIT() {
      super(Common.NASHORN_DEPS);
   }

   @BeforeAll
   public static void setup() {
      WORKING_DIR.mkdirs();
   }

   @AfterAll
   public static void teardown() {
      Util.recursiveFileRemove(WORKING_DIR);
   }

   @Test
   public void testManagerBackupUpload() throws Exception {
      String name = "testManagerBackup";
      performTest(
            client -> {
               RestContainerClient cm = client.container();
               assertStatus(ACCEPTED, cm.createBackup(name));
               return awaitOk(() -> cm.getBackup(name, false));
            },
            client -> await(client.container().deleteBackup(name)),
            (zip, client) -> {
               RestContainerClient cm = client.container();
               assertStatus(ACCEPTED, cm.restore(name, zip));
               return awaitCreated(() -> cm.getRestore(name));
            },
            this::assertWildcardContent,
            false
      );
   }

   @Test
   public void testManagerBackupFromFile() throws Exception {
      String name = "testManagerBackup";
      performTest(
            client -> {
               RestContainerClient cm = client.container();
               assertStatus(ACCEPTED, cm.createBackup(name));
               return awaitOk(() -> cm.getBackup(name, false));
            },
            client -> await(client.container().deleteBackup(name)),
            (zip, client) -> {
               RestContainerClient cm = client.container();
               assertStatus(ACCEPTED, cm.restore(name, zip.getPath(), null));
               return awaitCreated(() -> cm.getRestore(name));
            },
            this::assertWildcardContent,
            true
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

               RestContainerClient cm = client.container();
               assertStatus(ACCEPTED, cm.createBackup(name, params));
               return awaitOk(() -> cm.getBackup(name, false));
            },
            client -> await(client.container().deleteBackup(name)),
            (zip, client) -> {
               Map<String, List<String>> params = new HashMap<>();
               params.put("caches", Collections.singletonList("cache1"));
               params.put("counters", Collections.singletonList("*"));

               RestContainerClient cm = client.container();
               assertStatus(ACCEPTED, cm.restore(name, zip, params));
               return awaitCreated(() -> cm.getRestore(name));
            },
            client -> {
               // Assert that only caches and the specified "weak-volatile" counter have been backed up. Internal caches will still be present
               assertStatusAndBodyEquals(OK, "[\"" + PROTOBUF_METADATA_CACHE_NAME + "\",\"" + SCRIPT_CACHE_NAME + "\",\"cache1\"]", client.caches());
               assertStatusAndBodyEquals(OK, "[\"weak-volatile\"]", client.counters());
               assertStatus(NOT_FOUND, client.schemas().get("schema.proto"));
               assertStatusAndBodyEquals(OK, "[]", client.tasks().list(RestTaskClient.ResultType.USER));
            },
            false
      );
   }

   @Test
   public void testCreateDuplicateBackupResources() throws Exception {
      String backupName = "testCreateDuplicateBackupResources";
      // Start the source cluster
      startSourceCluster();
      RestClient client = source.getClient();

      populateContainer(client);

      RestContainerClient cm = client.container();
      assertStatus(ACCEPTED, cm.createBackup(backupName));

      assertStatus(CONFLICT, cm.createBackup(backupName));

      // Expect a 202 response as the previous backup will be in progress, so the request is accepted for now
      assertStatus(ACCEPTED, cm.deleteBackup(backupName));

      // Now wait until the backup has actually been deleted so that we successfully create another with the same name
      awaitStatus(() -> cm.deleteBackup(backupName), ACCEPTED, NO_CONTENT);
      assertStatus(NOT_FOUND, cm.deleteBackup(backupName));

      assertStatus(ACCEPTED, cm.createBackup(backupName));

      // Wait for the backup operation to finish
      awaitStatus(() -> cm.getBackup(backupName, false), ACCEPTED, OK);
      assertStatus(NO_CONTENT, cm.deleteBackup(backupName));
   }

   @Test
   public void testManagerRestoreParameters() throws Exception {
      String name = "testManagerRestoreParameters";
      performTest(
            client -> {
               // Create a backup of all container content
               RestContainerClient cm = client.container();
               assertStatus(ACCEPTED, cm.createBackup(name));
               return awaitOk(() -> cm.getBackup(name, false));
            },
            client -> await(client.container().deleteBackup(name)),
            (zip, client) -> {
               // Request that only the 'test.js' script is restored
               Map<String, List<String>> params = new HashMap<>();
               params.put("tasks", Collections.singletonList("scripts/test.js"));
               RestContainerClient cm = client.container();
               assertStatus(ACCEPTED, cm.restore(name, zip, params));
               return awaitCreated(() -> cm.getRestore(name));
            },
            client -> {
               // Assert that the test.js task has been restored
               List<Json> tasks = Json.read(assertStatus(OK, client.tasks().list(RestTaskClient.ResultType.USER))).asJsonList();
               assertEquals(1, tasks.size());
               assertEquals("scripts/test.js", tasks.iterator().next().at("name").asString());

               // Assert that no other content has been restored
               assertStatusAndBodyEquals(OK, "[\"" + PROTOBUF_METADATA_CACHE_NAME + "\",\"" + SCRIPT_CACHE_NAME + "\"]", client.caches());
               assertStatusAndBodyEquals(OK, "[]", client.counters());
               assertStatus(NOT_FOUND, client.schemas().get("schema.proto"));
            },
            false
      );
   }

   @Test
   public void testClusterBackupUpload() throws Exception {
      String name = "testClusterBackup";
      performTest(
            client -> {
               RestContainerClient cluster = client.container();
               assertStatus(ACCEPTED, cluster.createBackup(name));
               return awaitOk(() -> cluster.getBackup(name, false));
            },
            client -> await(client.container().deleteBackup(name)),
            (zip, client) -> {
               RestContainerClient c = client.container();
               assertStatus(ACCEPTED, c.restore(name, zip));
               return awaitCreated(() -> c.getRestore(name));
            },
            this::assertWildcardContent,
            false
      );
   }

   @Test
   public void testClusterBackupFromFile() throws Exception {
      String name = "testClusterBackup";
      performTest(
            client -> {
               RestContainerClient cluster = client.container();
               assertStatus(ACCEPTED, cluster.createBackup(name));
               return awaitOk(() -> cluster.getBackup(name, false));
            },
            client -> await(client.container().deleteBackup(name)),
            (zip, client) -> {
               RestContainerClient c = client.container();
               assertStatus(ACCEPTED, c.restore(name, zip.getPath()));
               return awaitCreated(() -> c.getRestore(name));
            },
            this::assertWildcardContent,
            true
      );
   }


   @Test
   public void testCLIPartialBackup() throws Exception {
      startSourceCluster();
      String backupName = "partial-backup";
      String fileName = backupName + ".zip";
      try (AeshTestConnection t = cli(source)) {
         t.send("backup create --templates=* -n " + backupName);
         // Ensure that the backup has finished before stopping the source cluster
         t.send("backup get --no-content " + backupName);
      }
      source.driver.syncFilesFromServer(0, "data");
      Path createdBackup = source.driver.getRootDir().toPath().resolve("0/data/backups").resolve(backupName).resolve(fileName);
      try (ZipFile zip = new ZipFile(createdBackup.toFile())) {
         // Ensure that only cache-configs are present
         assertNotNull(zipResourceDir(TEMPLATES));
         assertResourceDoesntExist(zip, CACHES, COUNTERS, PROTO_SCHEMAS, TASKS);
      }
      Files.delete(createdBackup);
   }

   private void assertResourceDoesntExist(ZipFile zip, BackupManager.Resources.Type... types) {
      for (BackupManager.Resources.Type type : types) {
         String dir = zipResourceDir(type);
         assertNull(zip.getEntry(dir));
      }
   }
   private String zipResourceDir(BackupManager.Resources.Type type) {
      return "containers/default/" + type.toString();
   }

   @Test
   public void testCLIBackupToCustomDir() {
      startSourceCluster();
      String backupName = "server-backup";
      String fileName = backupName + ".zip";
      File localBackupDir = new File(WORKING_DIR, "custom-dir");
      localBackupDir.mkdir();
      localBackupDir.setWritable(true, false);
      File serverBackupDir = new File(source.driver.syncFilesToServer(0, localBackupDir.getAbsolutePath()));
      source.driver.syncFilesToServer(1, localBackupDir.getAbsolutePath());

      try (AeshTestConnection t = cli(source)) {
         t.clear();
         t.send(format("backup create -d %s -n %s", serverBackupDir.getPath(), backupName));
         // Ensure that the backup has finished before stopping the source cluster
         t.send("backup get --no-content " + backupName);
      }
      localBackupDir = new File(source.driver.syncFilesFromServer(0, serverBackupDir.getAbsolutePath()));
      if (!localBackupDir.getName().equals("custom-dir")) {
         localBackupDir = new File(localBackupDir, "custom-dir");
      }
      assertTrue(new File(localBackupDir, backupName + "/" + fileName).exists());
   }

   @Test
   public void testCLIBackupFromServerDir() throws Exception {
      startSourceCluster();
      String backupName = "server-backup";
      String fileName = backupName + ".zip";
      try (AeshTestConnection t = cli(source)) {
         t.send("create cache --template=org.infinispan.DIST_SYNC backupCache");
         t.send("cd caches/backupCache");
         t.send("put k1 v1");
         t.send("ls");
         t.assertContains("k1");
         t.clear();
         t.send("backup create -n " + backupName);
         // Ensure that the backup has finished before stopping the source cluster
         t.send("backup get --no-content " + backupName);
      }

      source.driver.syncFilesFromServer(0, "data");

      stopSourceCluster();
      startTargetCluster();

      Path createdBackup = source.driver.getRootDir().toPath().resolve("0/data/backups").resolve(backupName).resolve(fileName);
      createdBackup = Paths.get(target.driver.syncFilesToServer(0, createdBackup.toString()));

      try (AeshTestConnection t = cli(target)) {
         t.send("backup restore " + createdBackup);
         Thread.sleep(1000);
         t.send("ls caches/backupCache");
         t.assertContains("k1");
      }
      Files.deleteIfExists(createdBackup);
   }

   @Test
   public void testCLIBackupUpload() throws Exception {
      startSourceCluster();
      Path createdBackup;
      try (AeshTestConnection t = cli(source)) {
         t.send("create cache --template=org.infinispan.DIST_SYNC backupCache");
         t.send("cd caches/backupCache");
         t.send("put k1 v1");
         t.send("ls");
         t.assertContains("k1");
         t.clear();
         String backupName = "example backup";
         t.send(format("backup create -n '%s'", backupName));
         t.send(format("backup get '%s'", backupName));
         Thread.sleep(1000);
         t.send(format("backup delete '%s'", backupName));
         String fileName = backupName + ".zip";
         createdBackup = Paths.get(System.getProperty("user.dir")).resolve(fileName);
      }

      stopSourceCluster();
      startTargetCluster();

      try (AeshTestConnection t = cli(target)) {
         t.send(format("backup restore -u '%s'", createdBackup));
         Thread.sleep(1000);
         t.send("ls caches/backupCache");
         t.assertContains("k1");
      }
      Files.delete(createdBackup);
   }

   private AeshTestConnection cli(Cluster cluster) {
      AeshTestConnection t = new AeshTestConnection();
      CLI.main(new AeshDelegatingShell(t), new Properties());
      String host = cluster.driver.getServerAddress(0).getHostAddress();
      int port = cluster.driver.getServerSocket(0, 11222).getPort();
      t.send(format("connect %s:%d", host, port));
      t.assertContains("//containers/default]>");
      t.clear();
      return t;
   }

   private static RestResponse awaitOk(Supplier<CompletionStage<RestResponse>> request) {
      return awaitResponse(request, ACCEPTED, OK);
   }

   private static RestResponse awaitCreated(Supplier<CompletionStage<RestResponse>> request) {
      return awaitResponse(request, ACCEPTED, CREATED);
   }

   private void performTest(Function<RestClient, RestResponse> backupAndDownload,
                            Function<RestClient, RestResponse> delete,
                            BiFunction<File, RestClient, RestResponse> restore,
                            Consumer<RestClient> assertTargetContent, boolean syncToServer) throws Exception {
      // Start the source cluster
      startSourceCluster();
      RestClient client = source.getClient();

      // Populate the source container
      populateContainer(client);

      // Perform the backup and download
      RestResponse getResponse = backupAndDownload.apply(client);
      String fileName = getResponse.header("Content-Disposition").split("=")[1];

      // Copy the returned zip bytes to the local working dir
      File backupZip = new File(WORKING_DIR, fileName);
      try (InputStream is = getResponse.bodyAsStream()) {
         Files.copy(is, backupZip.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
      getResponse.close();

      // Delete the backup from the server
      try (RestResponse deleteResponse = delete.apply(client)) {
         assertEquals(204, deleteResponse.status());
      }

      // Ensure that all of the backup files have been deleted from the source cluster
      // We must wait for a short period time here to ensure that the returned entity has actually been removed from the filesystem
      Thread.sleep(50);
      assertNoServerBackupFilesExist(source);

      // Shutdown the source cluster
      stopSourceCluster();

      // Start the target cluster
      startTargetCluster();
      client = target.getClient();

      if (syncToServer) {
         backupZip = new File(target.driver.syncFilesToServer(0, backupZip.getAbsolutePath()));
      }

      // Upload the backup to the target cluster
      try (RestResponse restoreResponse = restore.apply(backupZip, client)) {
         assertEquals(201, restoreResponse.status(), restoreResponse.body());
      }

      // Assert that all content has been restored as expected, connecting to the second node in the cluster to ensure
      // that configurations and caches are restored cluster-wide
      assertTargetContent.accept(target.getClient(1));

      // Ensure that the backup files have been deleted from the target cluster
      assertNoServerBackupFilesExist(target);
      stopTargetCluster();
   }

   private void populateContainer(RestClient client) throws Exception {
      String cacheName = "cache1";

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      createCache(cacheName, builder, client);

      RestCacheClient cache = client.cache(cacheName);
      for (int i = 0; i < NUM_ENTRIES; i++) {
         assertStatus(NO_CONTENT, cache.put(String.valueOf(i), "Val-" + i));
      }
      assertEquals(NUM_ENTRIES, getCacheSize(cacheName, client));

      createCounter("weak-volatile", Element.WEAK_COUNTER, Storage.VOLATILE, client, 0);
      createCounter("weak-persistent", Element.WEAK_COUNTER, Storage.PERSISTENT, client, -100);
      createCounter("strong-volatile", Element.STRONG_COUNTER, Storage.VOLATILE, client, 50);
      createCounter("strong-persistent", Element.STRONG_COUNTER, Storage.PERSISTENT, client, 0);

      addSchema(client);

      try (InputStream is = BackupManagerIT.class.getResourceAsStream("/scripts/test.js")) {
         String script = CommonsTestingUtil.loadFileAsString(is);
         assertStatus(OK, client.tasks().uploadScript("scripts/test.js", RestEntity.create(MediaType.APPLICATION_JAVASCRIPT, script)));
      }
   }

   private void assertWildcardContent(RestClient client) {
      String cacheName = "cache1";
      RestCacheClient cache = client.cache(cacheName);
      assertStatusAndBodyEquals(OK, Integer.toString(NUM_ENTRIES), cache.size());
      for (int i = 0; i < NUM_ENTRIES; i++) {
         String index = String.valueOf(i);
         assertStatusAndBodyEquals(OK, "Val-" + index, cache.get(index));
      }

      assertCounter(client, "weak-volatile", Element.WEAK_COUNTER, Storage.VOLATILE, 0);
      assertCounter(client, "weak-persistent", Element.WEAK_COUNTER, Storage.PERSISTENT, -100);
      assertCounter(client, "strong-volatile", Element.STRONG_COUNTER, Storage.VOLATILE, 50);
      assertCounter(client, "strong-persistent", Element.STRONG_COUNTER, Storage.PERSISTENT, 0);

      assertStatusAndBodyContains(OK, "message Person", client.schemas().get("schema.proto"));
      Json json = Json.read(assertStatus(OK, client.tasks().list(RestTaskClient.ResultType.USER)));
      assertTrue(json.isArray());
      List<Json> tasks = json.asJsonList();
      assertEquals(1, tasks.size());
      assertEquals("scripts/test.js", tasks.get(0).at("name").asString());
   }

   private void createCounter(String name, Element type, Storage storage, RestClient client, long delta) {
      String config = format("{\n" +
            "    \"%s\":{\n" +
            "        \"initial-value\":0,\n" +
            "        \"storage\":\"%s\"\n" +
            "    }\n" +
            "}", type, storage.toString());
      RestCounterClient counterClient = client.counter(name);
      assertStatus(OK, counterClient.create(RestEntity.create(MediaType.APPLICATION_JSON, config)));
      if (delta != 0) {
         assertNotNull(assertStatus(name.contains("strong") ? OK : NO_CONTENT, counterClient.add(delta)));
      }
   }

   private void assertCounter(RestClient client, String name, Element type, Storage storage, long expectedValue) {
      RestResponse rsp = await(client.counter(name).configuration());
      assertEquals(200, rsp.status());
      String content = rsp.body();
      Json config = Json.read(content).at(type.toString());
      assertEquals(storage.toString(), config.at("storage").asString());
      assertEquals(0, config.at("initial-value").asInteger());

      assertStatusAndBodyEquals(OK, Long.toString(expectedValue), client.counter(name).get());
   }

   static void assertNoServerBackupFilesExist(Cluster cluster) {
      for (int i = 0; i < 2; i++) {
         cluster.driver.syncFilesFromServer(i, "data");
         Path root = cluster.driver.getRootDir().toPath();
         File workingDir = root.resolve(Integer.toString(i)).resolve("data").resolve("backups").toFile();
         assertTrue(workingDir.isDirectory());
         String[] files = workingDir.list();
         assertNotNull(files);
         assertEquals(0, files.length, Arrays.toString(files));
      }
   }
}
