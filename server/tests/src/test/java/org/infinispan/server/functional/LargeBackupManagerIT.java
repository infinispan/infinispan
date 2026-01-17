package org.infinispan.server.functional;

import static org.infinispan.client.rest.RestResponseInfo.ACCEPTED;
import static org.infinispan.client.rest.RestResponseInfo.CREATED;
import static org.infinispan.client.rest.RestResponseInfo.NO_CONTENT;
import static org.infinispan.client.rest.RestResponseInfo.OK;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.server.functional.BackupManagerIT.assertNoServerBackupFilesExist;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.assertStatusAndBodyEquals;
import static org.infinispan.server.test.core.Common.awaitResponse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestContainerClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.testing.Testing;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class LargeBackupManagerIT extends AbstractMultiClusterIT {

   private static final File WORKING_DIR = new File(Testing.tmpDirectory(LargeBackupManagerIT.class));
   private final String name = "testManagerBackupUploadLargeCache";
   private final String cacheName = "cache1";
   private final int size = 35_000;

   @BeforeAll
   public static void setup() {
      WORKING_DIR.mkdirs();
   }

   @AfterAll
   public static void teardown() {
      Util.recursiveFileRemove(WORKING_DIR);
   }

   @Test
   public void testManagerBackupUploadLargeCache() throws Exception {
      // Start the source cluster
      startSourceCluster();
      RestClient client = source.getClient();

      // Populate the source container
      populateContainer(client);

      // Perform the backup and download
      RestResponse getResponse = backupAndDownload(client);
      String fileName = getResponse.header("Content-Disposition").split("=")[1];

      // Copy the returned zip bytes to the local working dir
      File backupZip = new File(WORKING_DIR, fileName);
      try (InputStream is = getResponse.bodyAsStream()) {
         Files.copy(is, backupZip.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
      getResponse.close();

      // Delete the backup from the server
      try (RestResponse deleteResponse = await(client.container().deleteBackup(name))) {
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

      // Upload the backup to the target cluster
      try (RestResponse restoreResponse = restore(backupZip, client)) {
         assertEquals(201, restoreResponse.status(), restoreResponse.body());
      }

      // Assert that all content has been restored as expected, connecting to the second node in the cluster to ensure
      // that configurations and caches are restored cluster-wide
      assertTargetContent(target.getClient(1));

      // Ensure that the backup files have been deleted from the target cluster
      assertNoServerBackupFilesExist(target);
      stopTargetCluster();
   }

   private void populateContainer(RestClient client) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      createCache(cacheName, builder, client);

      RestCacheClient cache = client.cache(cacheName);
      for (int i = 0; i < size; i++) {
         assertStatus(NO_CONTENT, cache.put("Key-" + i, UUID.randomUUID().toString()));
      }
      assertEquals(size, getCacheSize(cacheName, client));
   }

   private RestResponse backupAndDownload(RestClient client) {
      RestContainerClient cm = client.container();
      assertStatus(ACCEPTED, cm.createBackup(name));
      return awaitResponse(() -> cm.getBackup(name, false), ACCEPTED, OK);
   }

   private RestResponse restore(File zip, RestClient client) {
      RestContainerClient cm = client.container();
      assertStatus(ACCEPTED, cm.restore(name, zip));
      long start = System.currentTimeMillis();
      return awaitResponse(() -> {
         if (System.currentTimeMillis() - start > 10_000)
            fail("Failed to complete restore procedure on time");

         return cm.getRestore(name);
      }, ACCEPTED, CREATED);
   }

   private void assertTargetContent(RestClient client) {
      RestCacheClient cache = client.cache(cacheName);
      assertStatusAndBodyEquals(OK, Integer.toString(size), cache.size());
   }
}
