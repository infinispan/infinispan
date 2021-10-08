package org.infinispan.server.core.backup;

import static org.infinispan.server.core.backup.Constants.WORKING_DIR;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.lock.EmbeddedClusteredLockManagerFactory;
import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.server.core.BackupManager;
import org.infinispan.server.core.logging.Log;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 * @since 12.0
 */
public class BackupManagerImpl implements BackupManager {

   private static final Log log = LogFactory.getLog(BackupManagerImpl.class, Log.class);

   final ParserRegistry parserRegistry;
   final BlockingManager blockingManager;
   final Path rootDir;
   final BackupReader reader;
   final Lock backupLock;
   final Lock restoreLock;
   private final EmbeddedCacheManager cacheManager;
   final Map<String, DefaultCacheManager> cacheManagers;
   final Map<String, BackupRequest> backupMap;
   final Map<String, CompletionStage<Void>> restoreMap;

   public BackupManagerImpl(BlockingManager blockingManager, DefaultCacheManager cm, Path dataRoot) {
      this.blockingManager = blockingManager;
      this.rootDir = dataRoot.resolve(WORKING_DIR);
      this.cacheManager = cm;
      this.cacheManagers = Collections.singletonMap(cm.getName(), cm);
      this.parserRegistry = new ParserRegistry();
      this.reader = new BackupReader(blockingManager, cacheManagers, parserRegistry);
      this.backupLock = new Lock("backup", cm);
      this.restoreLock = new Lock("restore", cm);
      this.backupMap = new ConcurrentHashMap<>();
      this.restoreMap = new ConcurrentHashMap<>();
   }

   @Override
   public void init() throws IOException {
      Files.createDirectories(rootDir);
   }

   @Override
   public Set<String> getBackupNames() {
      SecurityActions.checkPermission(cacheManager.withSubject(Security.getSubject()), AuthorizationPermission.ADMIN);
      return new HashSet<>(backupMap.keySet());
   }

   @Override
   public Status getBackupStatus(String name) {
      SecurityActions.checkPermission(cacheManager.withSubject(Security.getSubject()), AuthorizationPermission.ADMIN);
      return getBackupStatus(backupMap.get(name));
   }

   @Override
   public Path getBackupLocation(String name) {
      SecurityActions.checkPermission(cacheManager.withSubject(Security.getSubject()), AuthorizationPermission.ADMIN);
      BackupRequest request = backupMap.get(name);
      Status status = getBackupStatus(request);
      if (status != Status.COMPLETE)
         return null;
      return request.future.join();
   }

   private Status getBackupStatus(BackupRequest request) {
      CompletableFuture<Path> future = request == null ? null : request.future;
      return getFutureStatus(future);
   }

   private Status getFutureStatus(CompletionStage<?> stage) {
      if (stage == null)
         return Status.NOT_FOUND;

      CompletableFuture<?> future = stage.toCompletableFuture();
      if (future.isCompletedExceptionally())
         return Status.FAILED;

      return future.isDone() ? Status.COMPLETE : Status.IN_PROGRESS;
   }

   @Override
   public CompletionStage<Status> removeBackup(String name) {
      SecurityActions.checkPermission(cacheManager.withSubject(Security.getSubject()), AuthorizationPermission.ADMIN);
      BackupRequest request = backupMap.remove(name);
      Status status = getBackupStatus(request);
      switch (status) {
         case NOT_FOUND:
            return CompletableFuture.completedFuture(status);
         case COMPLETE:
         case FAILED:
            return blockingManager.supplyBlocking(() -> {
               request.writer.cleanup();
               return Status.COMPLETE;
            }, "remove-completed-backup");
         case IN_PROGRESS:
            // The backup files are removed on exceptional or successful completion.
            blockingManager.handleBlocking(request.future, (path, t) -> {
               // Regardless of whether the backup completes exceptionally or successfully, we remove the files
               request.writer.cleanup();
               return null;
            }, "remove-inprogress-backup");
            return CompletableFuture.completedFuture(Status.IN_PROGRESS);
      }
      throw new IllegalStateException();
   }

   @Override
   public CompletionStage<Path> create(String name, Path workingDir) {
      return create(
            name,
            workingDir,
            cacheManagers.entrySet().stream()
                  .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        p -> new BackupManagerResources.Builder().includeAll().build()))
      );
   }

   @Override
   public CompletionStage<Path> create(String name, Path workingDir, Map<String, Resources> params) {
      SecurityActions.checkPermission(cacheManager.withSubject(Security.getSubject()), AuthorizationPermission.ADMIN);
      if (getBackupStatus(name) != Status.NOT_FOUND)
         return CompletableFutures.completedExceptionFuture(log.backupAlreadyExists(name));

      BackupWriter writer = new BackupWriter(name, blockingManager, cacheManagers, parserRegistry, workingDir == null ? rootDir : workingDir);
      CompletionStage<Path> backupStage = backupLock.lock()
            .thenCompose(lockAcquired -> {
               if (!lockAcquired)
                  return CompletableFutures.completedExceptionFuture(log.backupInProgress());

               log.initiatingBackup(name);
               return writer.create(params);
            });

      backupStage = CompletionStages.handleAndCompose(backupStage,
            (path, t) -> {
               CompletionStage<Void> unlock = backupLock.unlock();
               if (t != null) {
                  log.debug("Exception encountered when creating a cluster backup", t);
                  return unlock.thenCompose(ignore ->
                        CompletableFutures.completedExceptionFuture(log.errorCreatingBackup(t))
                  );
               }
               log.backupComplete(path.getFileName().toString());
               return unlock.thenCompose(ignore -> CompletableFuture.completedFuture(path));
            });

      backupMap.put(name, new BackupRequest(writer, backupStage));
      return backupStage;
   }

   @Override
   public CompletionStage<Status> removeRestore(String name) {
      SecurityActions.checkPermission(cacheManager.withSubject(Security.getSubject()), AuthorizationPermission.ADMIN);
      CompletionStage<Void> stage = restoreMap.remove(name);
      Status status = getFutureStatus(stage);
      return CompletableFuture.completedFuture(status);
   }

   @Override
   public Status getRestoreStatus(String name) {
      SecurityActions.checkPermission(cacheManager.withSubject(Security.getSubject()), AuthorizationPermission.ADMIN);
      return getFutureStatus(restoreMap.get(name));
   }

   @Override
   public Set<String> getRestoreNames() {
      SecurityActions.checkPermission(cacheManager.withSubject(Security.getSubject()), AuthorizationPermission.ADMIN);
      return new HashSet<>(restoreMap.keySet());
   }

   @Override
   public CompletionStage<Void> restore(String name, Path backup) {
      return restore(
            name,
            backup,
            cacheManagers.entrySet().stream()
                  .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        p -> new BackupManagerResources.Builder().includeAll().build()))
      );
   }

   @Override
   public CompletionStage<Void> restore(String name, Path backup, Map<String, Resources> params) {
      SecurityActions.checkPermission(cacheManager.withSubject(Security.getSubject()), AuthorizationPermission.ADMIN);
      if (getRestoreStatus(name) != Status.NOT_FOUND)
         return CompletableFutures.completedExceptionFuture(log.restoreAlreadyExists(name));

      if (!Files.exists(backup)) {
         CacheException e = log.errorRestoringBackup(backup, new FileNotFoundException(backup.toString()));
         log.error(e);
         return CompletableFutures.completedExceptionFuture(e);
      }

      CompletionStage<Void> restoreStage = restoreLock.lock()
            .thenCompose(lockAcquired -> {
               if (!lockAcquired)
                  return CompletableFutures.completedExceptionFuture(log.restoreInProgress());

               log.initiatingRestore(name, backup);
               return reader.restore(backup, params);
            });

      restoreStage = CompletionStages.handleAndCompose(restoreStage,
            (path, t) -> {
               CompletionStage<Void> unlock = restoreLock.unlock();
               if (t != null) {
                  log.debug("Exception encountered when restoring a cluster backup", t);
                  return unlock.thenCompose(ignore ->
                        CompletableFutures.completedExceptionFuture(log.errorRestoringBackup(backup, t))
                  );
               }
               log.restoreComplete(name);
               return unlock.thenCompose(ignore -> CompletableFuture.completedFuture(path));
            });
      restoreMap.put(name, restoreStage);
      return restoreStage;
   }

   static class BackupRequest {
      final BackupWriter writer;
      final CompletableFuture<Path> future;

      BackupRequest(BackupWriter writer, CompletionStage<Path> stage) {
         this.writer = writer;
         this.future = stage.toCompletableFuture();
      }
   }

   static class Lock {
      final String name;
      final EmbeddedCacheManager cm;
      final boolean isClustered;
      volatile ClusteredLock clusteredLock;
      volatile AtomicBoolean localLock;

      Lock(String name, EmbeddedCacheManager cm) {
         this.name = String.format("%s-%s", BackupManagerImpl.class.getSimpleName(), name);
         this.cm = cm;
         this.isClustered = SecurityActions.getGlobalConfiguration(cm).isClustered();
      }

      CompletionStage<Boolean> lock() {
         if (isClustered)
            return getClusteredLock().tryLock();

         return CompletableFuture.completedFuture(getLocalLock().compareAndSet(false, true));
      }

      CompletionStage<Void> unlock() {
         if (isClustered)
            return getClusteredLock().unlock();

         getLocalLock().compareAndSet(true, false);
         return CompletableFutures.completedNull();
      }

      private ClusteredLock getClusteredLock() {
         if (clusteredLock == null) {
            synchronized (this) {
               if (clusteredLock == null) {
                  ClusteredLockManager lockManager = EmbeddedClusteredLockManagerFactory.from(cm);
                  boolean isDefined = lockManager.isDefined(name);
                  if (!isDefined) {
                     lockManager.defineLock(name);
                  }
                  clusteredLock = lockManager.get(name);
               }
            }
         }
         return clusteredLock;
      }

      private AtomicBoolean getLocalLock() {
         if (localLock == null)
            localLock = new AtomicBoolean();
         return localLock;
      }
   }
}
