package org.infinispan.server.core.backup;

import static org.infinispan.server.core.backup.Constants.CONTAINERS_PROPERTIES_FILE;
import static org.infinispan.server.core.backup.Constants.CONTAINER_KEY;
import static org.infinispan.server.core.backup.Constants.GLOBAL_CONFIG_FILE;
import static org.infinispan.server.core.backup.Constants.MANIFEST_PROPERTIES_FILE;
import static org.infinispan.server.core.backup.Constants.VERSION_KEY;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.BackupManager;
import org.infinispan.server.core.backup.resources.ContainerResourceFactory;
import org.infinispan.server.core.logging.Log;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.LogFactory;

/**
 * Responsible for creating backup files that can be used to restore a container/cache on a new cluster.
 *
 * @author Ryan Emerson
 * @since 12.0
 */
class BackupWriter {

   private static final Log log = LogFactory.getLog(BackupWriter.class, Log.class);

   private final String name;
   private final BlockingManager blockingManager;
   private final Map<String, DefaultCacheManager> cacheManagers;
   private final ParserRegistry parserRegistry;
   private final Path rootDir;

   BackupWriter(String name, BlockingManager blockingManager, Map<String, DefaultCacheManager> cacheManagers,
                ParserRegistry parserRegistry, Path rootDir) {
      this.name = name;
      this.blockingManager = blockingManager;
      this.cacheManagers = cacheManagers;
      this.parserRegistry = parserRegistry;
      this.rootDir = rootDir.resolve(name);
   }

   void cleanup() {
      log.backupDeleted(name);
      Util.recursiveFileRemove(rootDir);
   }

   CompletionStage<Path> create(Map<String, BackupManager.Resources> params) {
      AggregateCompletionStage<Void> stages = CompletionStages.aggregateCompletionStage();
      for (Map.Entry<String, BackupManager.Resources> e : params.entrySet()) {
         String containerName = e.getKey();
         EmbeddedCacheManager cm = cacheManagers.get(containerName);
         stages.dependsOn(createBackup(containerName, cm, e.getValue()));
      }

      stages.dependsOn(writeManifest(cacheManagers.keySet()));
      return blockingManager.thenApplyBlocking(stages.freeze(), Void -> createZip(), "create");
   }

   /**
    * Create a backup of the specified container.
    *
    * @param containerName the name of container to backup.
    * @param cm            the container to backup.
    * @param params        the {@link BackupManager.Resources} object that determines what resources are included in the
    *                      backup for this container.
    * @return a {@link CompletionStage} that completes once the backup has finished.
    */
   private CompletionStage<Void> createBackup(String containerName, EmbeddedCacheManager cm, BackupManager.Resources params) {
      Path containerRoot = rootDir.resolve(CONTAINER_KEY).resolve(containerName);
      try {
         Files.createDirectories(containerRoot);
      } catch (IOException e) {
         throw new CacheException("Unable to create directories " + containerRoot.toString());
      }
      GlobalComponentRegistry gcr = cm.getGlobalComponentRegistry();
      BlockingManager blockingManager = gcr.getComponent(BlockingManager.class);

      Collection<ContainerResource> resources = ContainerResourceFactory
            .getResources(params, blockingManager, cm, parserRegistry, containerRoot);

      // Prepare and ensure all requested resources are valid before starting the backup process
      resources.forEach(ContainerResource::prepareAndValidateBackup);

      AggregateCompletionStage<Void> stages = CompletionStages.aggregateCompletionStage();
      for (ContainerResource cr : resources)
         stages.dependsOn(cr.backup());

      stages.dependsOn(
            // Write the global configuration xml
            blockingManager.runBlocking(() ->
                  writeGlobalConfig(cm.getCacheManagerConfiguration(), containerRoot), "global-config")
      );

      return blockingManager.thenRunBlocking(
            stages.freeze(),
            () -> {
               Properties manifest = new Properties();
               resources.forEach(r -> r.writeToManifest(manifest));
               storeProperties(manifest, "Container Properties", containerRoot.resolve(CONTAINERS_PROPERTIES_FILE));
            },
            "create-manifest"
      );
   }

   private CompletionStage<Void> writeManifest(Set<String> containers) {
      return blockingManager.runBlocking(() -> {
         Properties manifest = new Properties();
         manifest.put(CONTAINER_KEY, String.join(",", containers));
         manifest.put(VERSION_KEY, Version.getVersion());
         storeProperties(manifest, "Backup Manifest", rootDir.resolve(MANIFEST_PROPERTIES_FILE));
      }, "write-manifest");
   }

   private void writeGlobalConfig(GlobalConfiguration configuration, Path root) {
      Path xmlPath = root.resolve(GLOBAL_CONFIG_FILE);
      try (OutputStream os = Files.newOutputStream(xmlPath)) {
         parserRegistry.serialize(os, configuration, Collections.emptyMap());
      } catch (XMLStreamException | IOException e) {
         throw new CacheException(String.format("Unable to create global configuration file '%s'", xmlPath), e);
      }
   }

   private Path createZip() {
      Path zipFile = rootDir.resolve(name + ".zip");
      try (ZipOutputStream zs = new ZipOutputStream(Files.newOutputStream(Files.createFile(zipFile)))) {
         Files.walkFileTree(rootDir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
               if (!path.equals(zipFile)) {
                  String name = rootDir.relativize(path).toString();
                  zs.putNextEntry(new ZipEntry(name));
                  Files.copy(path, zs);
                  zs.closeEntry();
                  Files.delete(path);
               }
               return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
               if (exc != null)
                  throw new IllegalStateException(exc);

               if (!dir.equals(rootDir))
                  Files.delete(dir);
               return FileVisitResult.CONTINUE;
            }
         });
      } catch (IOException e) {
         throw new CacheException(e);
      }
      return zipFile;
   }

   private void storeProperties(Properties properties, String description, Path dest) {
      try (OutputStream os = Files.newOutputStream(dest)) {
         properties.store(os, description);
      } catch (IOException e) {
         throw new CacheException(String.format("Unable to create %s file", description), e);
      }
   }
}
