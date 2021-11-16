package org.infinispan.server.functional;

import static org.infinispan.server.core.BackupManager.Resources.Type.CACHES;
import static org.infinispan.server.core.BackupManager.Resources.Type.COUNTERS;
import static org.infinispan.server.core.BackupManager.Resources.Type.PROTO_SCHEMAS;
import static org.infinispan.server.core.BackupManager.Resources.Type.TASKS;
import static org.infinispan.server.core.BackupManager.Resources.Type.TEMPLATES;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.zip.ZipFile;

import org.infinispan.cli.commands.CLI;
import org.infinispan.cli.impl.AeshDelegatingShell;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.server.core.BackupManager;
import org.infinispan.server.test.core.AeshTestConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Ryan Emerson
 * @since 12.0
 */
public class CliBackupManagerIT extends AbstractMultiClusterIT {
   static final File WORKING_DIR = new File(CommonsTestingUtil.tmpDirectory(CliBackupManagerIT.class));

   public CliBackupManagerIT() {
      super("configuration/ClusteredServerTest.xml");
   }

   @Before
   public void setup() {
      WORKING_DIR.mkdirs();
   }

   @After
   @Override
   public void cleanup() throws Exception {
      super.cleanup();
      Util.recursiveFileRemove(WORKING_DIR);
   }

   @Test
   public void testPartialBackup() throws Exception {
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
   public void testBackupToCustomDir() {
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
         t.send(String.format("backup create -d %s -n %s", serverBackupDir.getPath(), backupName));
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
   public void testBackupFromServerDir() throws Exception {
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
   public void testBackupUpload() throws Exception {
      startSourceCluster();
      Path createdBackup;
      try (AeshTestConnection t = cli(source)) {
         t.send("create cache --template=org.infinispan.DIST_SYNC backupCache");
         t.send("cd caches/backupCache");
         t.send("put k1 v1");
         t.send("ls");
         t.assertContains("k1");
         t.clear();
         String backupName = "example-backup";
         t.send("backup create -n " + backupName);
         t.send("backup get " + backupName);
         Thread.sleep(1000);
         t.send("backup delete " + backupName);
         String fileName = backupName + ".zip";
         createdBackup = Paths.get(System.getProperty("user.dir")).resolve(fileName);
      }

      stopSourceCluster();
      startTargetCluster();

      try (AeshTestConnection t = cli(target)) {
         t.send("backup restore -u " + createdBackup);
         Thread.sleep(1000);
         t.send("ls caches/backupCache");
         t.assertContains("k1");
      }
      Files.delete(createdBackup);
   }

   private AeshTestConnection cli(Cluster cluster) {
      AeshTestConnection t = new AeshTestConnection();
      CLI.main(new AeshDelegatingShell(t), new String[]{}, new Properties());
      String host = cluster.driver.getServerAddress(0).getHostAddress();
      int port = cluster.driver.getServerSocket(0, 11222).getPort();
      t.send(String.format("connect %s:%d", host, port));
      t.assertContains("//containers/default]>");
      t.clear();
      return t;
   }
}
