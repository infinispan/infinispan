package org.infinispan.query.impl;

import static org.infinispan.query.impl.config.SearchPropertyExtractor.getIndexLocation;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.IndexStartupMode;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.query.Indexer;
import org.infinispan.query.core.impl.Log;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.jspecify.annotations.NonNull;

public final class IndexStartupRunner {

   private static final Log log = Log.getLog(IndexStartupRunner.class);
   private final AdvancedCache<?, ?> cache;

   public IndexStartupRunner(AdvancedCache<?, ?> cache) {
      this.cache = cache;
   }

   public void run() {
      Configuration configuration = cache.getCacheConfiguration();
      Path path = getIndexLocation(cache.getCacheManager().getCacheManagerConfiguration(), configuration.indexing().path(), cache.getName());
      AtomicBoolean zapped = new AtomicBoolean();
      if (Files.exists(path)) {
         // Attempt to read indexes to ensure compatibility with the current version of Lucene
         try {
            Files.walkFileTree(path, new SimpleFileVisitor<>() {
               @Override
               public @NonNull FileVisitResult postVisitDirectory(@NonNull Path dir, IOException exc) throws IOException {
                  if (exc != null) throw exc;
                  // Check if this directory has any subdirectories
                  try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
                     boolean hasSubDir = false;
                     for (Path entry : stream) {
                        if (Files.isDirectory(entry)) {
                           hasSubDir = true;
                           break;
                        }
                     }
                     if (!hasSubDir) {
                        try (Directory directory = FSDirectory.open(dir)) {
                           SegmentInfos.readLatestCommit(directory);
                        } catch (IllegalArgumentException | IndexFormatTooNewException | IndexFormatTooOldException e) {
                           Log.CONTAINER.incompatibleIndexes(dir);
                           zapped.set(true);
                           Util.recursiveFileRemove(dir);
                           Files.createDirectories(dir);
                        } catch (IndexNotFoundException e) {
                           // Ignore this
                        }
                     }
                  }
                  return FileVisitResult.CONTINUE;
               }
            });
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      }
      IndexStartupMode startupMode = computeFinalMode(configuration, zapped.get());
      SearchMapping mapping = ComponentRegistry.of(cache).getComponent(SearchMapping.class);
      Indexer indexer = ComponentRegistry.of(cache).getComponent(Indexer.class);
      if (IndexStartupMode.PURGE.equals(startupMode)) {
         mapping.scopeAll().workspace().purge();
      } else if (IndexStartupMode.REINDEX.equals(startupMode)) {
         indexer.runLocal();
      }
   }

   private IndexStartupMode computeFinalMode(Configuration configuration, boolean zapped) {
      IndexStartupMode startupMode = configuration.indexing().startupMode();
      DataKind dataKind = computeDataKind(configuration);
      boolean indexesAreVolatile = IndexStorage.LOCAL_HEAP.equals(configuration.indexing().storage());

      if (DataKind.VOLATILE.equals(dataKind) && !indexesAreVolatile) {
         switch (startupMode) {
            case AUTO, PURGE,
                 // reindex is equivalent to purge, since there is no data in the caches
                 REINDEX -> {
               return IndexStartupMode.PURGE;
            }
            default -> {
               log.logIndexStartupModeMismatch("volatile", "persistent", startupMode.toString());
               return IndexStartupMode.NONE;
            }
         }
      }

      if (DataKind.PERSISTENT.equals(dataKind) && (indexesAreVolatile || zapped)) {
         switch (startupMode) {
            case AUTO, REINDEX -> {
               return IndexStartupMode.REINDEX;
            }
            default -> {
               log.logIndexStartupModeMismatch("persistent", "volatile", startupMode.toString());
               return startupMode;
            }
         }
      }

      if (DataKind.SHARED_STORE.equals(dataKind) && !indexesAreVolatile && IndexStartupMode.AUTO.equals(startupMode)) {
         // @fax4ever: I'm against this. In my opinion run always a reindex, even if
         // configuration.memory().isEvictionEnabled() is false, is wrong.
         // This may penalize the average user using a shared cache store that does not do any eviction!
         // But since the others of the team want this at all cost, I give up in the spirit of collaboration.
         return IndexStartupMode.REINDEX;
      }

      return (IndexStartupMode.AUTO.equals(startupMode)) ? IndexStartupMode.NONE : startupMode;
   }

   private enum DataKind {
      VOLATILE, PERSISTENT, SHARED_STORE
   }

   private DataKind computeDataKind(Configuration configuration) {
      List<StoreConfiguration> cacheStores = configuration.persistence().stores();
      if (cacheStores.isEmpty()) {
         return DataKind.VOLATILE;
      }

      boolean sharedStore = false;
      for (StoreConfiguration cacheStore : cacheStores) {
         if (cacheStore.purgeOnStartup()) {
            continue;
         }
         if (cacheStore.shared()) {
            // @fax4ever: I'm against this. In my opinion run always a reindex, even if
            // configuration.memory().isEvictionEnabled() is false, is wrong.
            // This may penalize the average user using a shared cache store that does not do any eviction!
            // But since the others of the team want this at all cost, I give up in the spirit of collaboration.
            sharedStore = true;
         } else {
            return DataKind.PERSISTENT;
         }
      }

      if (sharedStore) {
         return DataKind.SHARED_STORE;
      }
      return DataKind.VOLATILE;
   }
}
