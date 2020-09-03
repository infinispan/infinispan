package org.infinispan.server.functional;

import static org.infinispan.server.core.BackupManager.Resources.Type.CACHES;
import static org.infinispan.server.core.BackupManager.Resources.Type.CACHE_CONFIGURATIONS;
import static org.infinispan.server.core.BackupManager.Resources.Type.COUNTERS;
import static org.infinispan.server.core.BackupManager.Resources.Type.PROTO_SCHEMAS;
import static org.infinispan.server.core.BackupManager.Resources.Type.SCRIPTS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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
         t.readln("backup create --cache-configs=* -n " + backupName);
         // Ensure that the backup has finished before stopping the source cluster
         t.readln("backup get --no-content " + backupName);
      }
      Path createdBackup = source.driver.getRootDir().toPath().resolve("0/data/backup-manager").resolve(backupName).resolve(fileName);
      try (ZipFile zip = new ZipFile(createdBackup.toFile())) {
         // Ensure that only cache-configs are present
         assertNotNull(zipResourceDir(CACHE_CONFIGURATIONS));
         assertResourceDoesntExist(zip, CACHES, COUNTERS, PROTO_SCHEMAS, SCRIPTS);
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
   public void testBackupFromServerDir() throws Exception {
      startSourceCluster();
      String backupName = "server-backup";
      String fileName = backupName + ".zip";
      try (AeshTestConnection t = cli(source)) {
         t.readln("create cache --template=org.infinispan.DIST_SYNC backupCache");
         t.readln("cd caches/backupCache");
         t.readln("put k1 v1");
         t.readln("ls");
         t.assertContains("k1");
         t.clear();
         t.readln("backup create -n " + backupName);
         // Ensure that the backup has finished before stopping the source cluster
         t.readln("backup get --no-content " + backupName);
      }

      stopSourceCluster();
      startTargetCluster();

      // The location of the created .zip file that in EMBEDDED mode is available to both the source and target server
      // TODO This won't work with CONTAINER mode, we need a way to copy the file between the container filesystems at runtime
      Path createdBackup = source.driver.getRootDir().toPath().resolve("0/data/backup-manager").resolve(backupName).resolve(fileName);
      try (AeshTestConnection t = cli(target)) {
         t.readln("backup restore " + createdBackup.toString());
         Thread.sleep(1000);
         t.readln("ls caches/backupCache");
         t.assertContains("k1");
      }
      Files.delete(createdBackup);
   }

   @Test
   public void testBackupUpload() throws Exception {
      startSourceCluster();
      Path createdBackup;
      try (AeshTestConnection t = cli(source)) {
         t.readln("create cache --template=org.infinispan.DIST_SYNC backupCache");
         t.readln("cd caches/backupCache");
         t.readln("put k1 v1");
         t.readln("ls");
         t.assertContains("k1");
         t.clear();
         String backupName = "example-backup";
         t.readln("backup create -n " + backupName);
         t.readln("backup get " + backupName);
         Thread.sleep(1000);
         t.readln("backup delete " + backupName);
         String fileName = backupName + ".zip";
         createdBackup = Paths.get(System.getProperty("user.dir")).resolve(fileName);
      }

      stopSourceCluster();
      startTargetCluster();

      try (AeshTestConnection t = cli(target)) {
         t.readln("backup restore -u " + createdBackup);
         Thread.sleep(1000);
         t.readln("ls caches/backupCache");
         t.assertContains("k1");
      }
      Files.delete(createdBackup);
   }

   private AeshTestConnection cli(Cluster cluster) {
      AeshTestConnection t = new AeshTestConnection();
      CLI.main(new AeshDelegatingShell(t), new String[]{}, new Properties());
      String host = cluster.driver.getServerAddress(0).getHostAddress();
      int port = cluster.driver.getServerSocket(0, 11222).getPort();
      t.readln(String.format("connect %s:%d", host, port));
      t.assertContains("//containers/default]>");
      t.clear();
      return t;
   }
}
