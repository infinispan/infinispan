package org.infinispan.interceptors.impl;

import static org.infinispan.util.IracUtils.logUpdateDiscarded;
import static org.infinispan.util.IracUtils.setPrivateMetadata;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.IracTombstoneManager;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.Ownership;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationSuccessAction;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.transaction.impl.WriteSkewHelper;
import org.infinispan.util.logging.LogSupplier;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.spi.SiteEntry;
import org.infinispan.xsite.spi.XSiteEntryMergePolicy;

/**
 * Interceptor to handle updates from remote sites.
 * <p>
 * Remote sites only send {@link PutKeyValueCommand} or {@link RemoveCommand}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class NonTxIracRemoteSiteInterceptor extends DDAsyncInterceptor implements LogSupplier {

   private static final Log log = LogFactory.getLog(NonTxIracRemoteSiteInterceptor.class);
   private final boolean needsVersions;
   private final InvocationSuccessAction<DataWriteCommand> setMetadataForOwnerAction = this::setIracMetadataForOwner;
   @Inject XSiteEntryMergePolicy<Object, Object> mergePolicy;
   @Inject IracVersionGenerator iracVersionGenerator;
   @Inject IracTombstoneManager iracTombstoneManager;
   @Inject VersionGenerator versionGenerator;
   @Inject ClusteringDependentLogic clusteringDependentLogic;

   public NonTxIracRemoteSiteInterceptor(boolean needsVersions) {
      this.needsVersions = needsVersions;
   }

   private static SiteEntry<Object> createSiteEntryFrom(CacheEntry<?, ?> entry, String siteName) {
      assert entry instanceof MVCCEntry;
      MVCCEntry<?, ?> mvccEntry = (MVCCEntry<?, ?>) entry;
      return new SiteEntry<>(siteName, mvccEntry.getOldValue(), mvccEntry.getOldMetadata());
   }

   @Override
   public Object visitIracPutKeyValueCommand(InvocationContext ctx, IracPutKeyValueCommand command) {
      Ownership ownership = getOwnership(command.getSegment());

      switch (ownership) {
         case PRIMARY:
            //we are on primary and the lock is acquired
            //if the update is discarded, command.isSuccessful() will return false.
            CompletionStage<Boolean> validationResult = validateOnPrimary(ctx, command);
            if (CompletionStages.isCompletedSuccessfully(validationResult)) {
               return validate(validationResult.toCompletableFuture().join(), ctx, command);
            }
            return validationResult.thenApply(isValid -> validate(isValid, ctx, command));
         case BACKUP:
            if (!ctx.isOriginLocal()) {
               //backups only commit when the command are remote (i.e. after validated from the originator)
               return invokeNextThenAccept(ctx, command, setMetadataForOwnerAction);
            }
      }
      return invokeNext(ctx, command);
   }

   @Override
   public boolean isTraceEnabled() {
      return log.isTraceEnabled();
   }

   @Override
   public Log getLog() {
      return log;
   }

   private Object validate(boolean isValid, InvocationContext ctx, DataWriteCommand command) {
      return isValid ? invokeNextThenAccept(ctx, command, setMetadataForOwnerAction) : null;
   }

   /**
    * Invoked on the primary owner, it validates if the remote site update is valid or not.
    * <p>
    * It also performs a conflict resolution if a conflict is found.
    */
   private CompletionStage<Boolean> validateOnPrimary(InvocationContext ctx, IracPutKeyValueCommand command) {
      final Object key = command.getKey();
      CacheEntry<?, ?> entry = ctx.lookupEntry(key);
      IracMetadata localMetadata = getIracMetadata(entry);

      if (localMetadata == null) {
         localMetadata = iracTombstoneManager.getTombstone(key);
      }

      if (needsVersions) {
         //we are in the primary owner with the lock acquired.
         //create a version for write-skew check before validating and sending to backup owners.
         PrivateMetadata metadata = PrivateMetadata.getBuilder(command.getInternalMetadata())
               .entryVersion(generateWriteSkewVersion(entry))
               .build();
         command.setInternalMetadata(key, metadata);
      }

      if (localMetadata != null) {
         return validateRemoteUpdate(entry, command, localMetadata);
      }
      return CompletableFutures.completedTrue();
   }

   /**
    * Invoked by backup owners, it makes sure the entry has the same version as set by the primary owner.
    */
   private void setIracMetadataForOwner(InvocationContext ctx, DataWriteCommand command,
         @SuppressWarnings("unused") Object rv) {
      final Object key = command.getKey();
      PrivateMetadata metadata = command.getInternalMetadata();
      iracVersionGenerator.updateVersion(command.getSegment(), metadata.iracMetadata().getVersion());
      setPrivateMetadata(ctx.lookupEntry(key), command.getSegment(), metadata, iracTombstoneManager, this);
   }

   private CompletionStage<Boolean> validateRemoteUpdate(CacheEntry<?, ?> entry, IracPutKeyValueCommand command,
         IracMetadata localMetadata) {
      IracMetadata remoteMetadata = command.getInternalMetadata().iracMetadata();
      assert remoteMetadata != null;
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] Comparing local and remote metadata: %s and %s", localMetadata, remoteMetadata);
      }
      IracEntryVersion localVersion = localMetadata.getVersion();
      IracEntryVersion remoteVersion = remoteMetadata.getVersion();
      if (command.isExpiration()) {
         // expiration rules
         // if the version is newer of equals, then the remove can continue
         // if not, or if there is a conflict, we abort
         switch (remoteVersion.compareTo(localVersion)) {
            case AFTER:
            case EQUAL:
               return CompletableFutures.completedTrue();
            default:
               discardUpdate(entry, command, remoteMetadata);
               return CompletableFutures.completedFalse();
         }
      }
      switch (remoteVersion.compareTo(localVersion)) {
         case CONFLICTING:
            return resolveConflict(entry, command, localMetadata, remoteMetadata);
         case EQUAL:
         case BEFORE:
            discardUpdate(entry, command, remoteMetadata);
            return CompletableFutures.completedFalse();
      }
      return CompletableFutures.completedTrue();
   }

   private CompletionStage<Boolean> resolveConflict(CacheEntry<?, ?> entry, IracPutKeyValueCommand command,
         IracMetadata localMetadata, IracMetadata remoteMetadata) {
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] Conflict found between local and remote metadata: %s and %s", localMetadata,
               remoteMetadata);
      }
      //same site? conflict?
      SiteEntry<Object> localSiteEntry = createSiteEntryFrom(entry, localMetadata.getSite());
      SiteEntry<Object> remoteSiteEntry = command.createSiteEntry(remoteMetadata.getSite());

      return mergePolicy.merge(entry.getKey(), localSiteEntry, remoteSiteEntry).thenApply(resolved -> {
         if (log.isTraceEnabled()) {
            log.tracef("[IRAC] resolve(%s, %s) = %s", localSiteEntry, remoteSiteEntry, resolved);
         }
         //fast track, it is the same entry as stored already locally. do nothing!
         if (resolved.equals(localSiteEntry)) {
            discardUpdate(entry, command, remoteMetadata);
            return false;
         } else if (!resolved.equals(remoteSiteEntry)) {
            //new value/metadata to store. Change the command!
            Object key = entry.getKey();
            command.updateCommand(resolved);
            PrivateMetadata.Builder builder = PrivateMetadata.getBuilder(command.getInternalMetadata())
                  .iracMetadata(mergeVersion(resolved.getSiteName(), localMetadata.getVersion(), remoteMetadata.getVersion()));
            command.setInternalMetadata(key, builder.build());
         }
         return true;
      });
   }

   private IracMetadata mergeVersion(String siteName, IracEntryVersion localVersion, IracEntryVersion remoteVersion) {
      return new IracMetadata(siteName, localVersion.merge(remoteVersion));
   }

   private IncrementableEntryVersion generateWriteSkewVersion(CacheEntry<?, ?> entry) {
      IncrementableEntryVersion version = WriteSkewHelper.incrementVersion(entry, versionGenerator);
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] Generated Write Skew version for %s=%s", entry.getKey(), version);
      }
      return version;
   }


   private void discardUpdate(CacheEntry<?, ?> entry, DataWriteCommand command, IracMetadata metadata) {
      final Object key = entry.getKey();
      logUpdateDiscarded(key, metadata, this);
      assert metadata != null : "[IRAC] Metadata must not be null!";
      command.fail(); //this prevents the sending to the backup owners
      entry.setChanged(false); //this prevents the local node to apply the changes.
      //we are discarding the update but try to make it visible for the next write operation
      iracVersionGenerator.updateVersion(command.getSegment(), metadata.getVersion());
   }

   private IracMetadata getIracMetadata(CacheEntry<?, ?> entry) {
      PrivateMetadata privateMetadata = entry.getInternalMetadata();
      if (privateMetadata == null) { // new entry!
         return iracTombstoneManager.getTombstone(entry.getKey());
      }
      IracMetadata metadata = privateMetadata.iracMetadata();
      return metadata == null ? iracTombstoneManager.getTombstone(entry.getKey()) : metadata;
   }

   private Ownership getOwnership(int segment) {
      return clusteringDependentLogic.getCacheTopology().getSegmentDistribution(segment).writeOwnership();
   }
}
