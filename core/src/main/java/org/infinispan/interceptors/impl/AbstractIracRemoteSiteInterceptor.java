package org.infinispan.interceptors.impl;

import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * An IRAC related interceptor that that handles the requests from the remote site.
 * <p>
 * This class contains only the common code and it is always present in the {@link AsyncInterceptorChain} because the
 * cache has no knowledge if it is a backup from others site or not.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public abstract class AbstractIracRemoteSiteInterceptor extends AbstractIracLocalSiteInterceptor {

   /**
    * Invoked on the primary owner, it validates if the remote site update is valid or not.
    * <p>
    * It also performs a conflict resolution if a conflict is found.
    */
   protected void validateOnPrimary(InvocationContext ctx, DataWriteCommand command,
         @SuppressWarnings("unused") Object rv) {
      final Object key = command.getKey();
      CacheEntry<?, ?> entry = ctx.lookupEntry(key);
      IracMetadata remoteMetadata = command.getInternalMetadata(key).iracMetadata();
      IracMetadata localMetadata = getIracMetadata(entry);

      if (localMetadata == null) {
         localMetadata = iracVersionGenerator.getTombstone(key);
      }

      assert remoteMetadata != null;

      iracVersionGenerator.updateVersion(getSegment(command, key), remoteMetadata.getVersion());

      if (localMetadata != null) {
         validateAndSetMetadata(entry, command, localMetadata, remoteMetadata);
      } else {
         setIracMetadata(entry, remoteMetadata);
      }
   }

   /**
    * Invoked by backup owners, it make sure the entry has the same version as set by the primary owner.
    */
   protected void setIracMetadataForOwner(InvocationContext ctx, DataWriteCommand command,
         @SuppressWarnings("unused") Object rv) {
      final Object key = command.getKey();
      IracMetadata metadata = command.getInternalMetadata(key).iracMetadata();
      assert metadata != null;
      iracVersionGenerator.updateVersion(getSegment(command, key), metadata.getVersion());
      setIracMetadata(ctx.lookupEntry(key), metadata);
   }

   private void validateAndSetMetadata(CacheEntry<?, ?> entry, DataWriteCommand command,
         IracMetadata localMetadata, IracMetadata remoteMetadata) {
      if (isTraceEnabled()) {
         getLog().tracef("[IRAC] Comparing local and remote metadata: %s and %s", localMetadata, remoteMetadata);
      }
      IracEntryVersion localVersion = localMetadata.getVersion();
      IracEntryVersion remoteVersion = remoteMetadata.getVersion();
      switch (remoteVersion.compareTo(localVersion)) {
         case CONFLICTING:
            resolveConflict(entry, command, localMetadata, remoteMetadata);
            return;
         case EQUAL:
         case BEFORE:
            discardUpdate(entry, command, remoteMetadata);
            return;
      }
      setIracMetadata(entry, remoteMetadata);
   }


   private void resolveConflict(CacheEntry<?, ?> entry, DataWriteCommand command, IracMetadata localMetadata,
         IracMetadata remoteMetadata) {
      if (isTraceEnabled()) {
         getLog().tracef("[IRAC] Conflict found between local and remote metadata: %s and %s", localMetadata,
               remoteMetadata);
      }
      //same site? conflict?
      // TODO conflict resolution https://issues.redhat.com/browse/ISPN-11802
      assert !localMetadata.getSite().equals(remoteMetadata.getSite());
      if (localMetadata.getSite().compareTo(remoteMetadata.getSite()) < 0) {
         discardUpdate(entry, command, remoteMetadata);
         return;
      }
      //other site update has priority!
      setIracMetadata(entry, remoteMetadata);
      //TODO! this isn't required now but when we allow custom conflict resolution, we need to merge the versions!
      //updateCommandMetadata(entry.getKey(), command, remoteMetadata);
   }

   private void discardUpdate(CacheEntry<?, ?> entry, DataWriteCommand command, IracMetadata metadata) {
      final Object key = entry.getKey();
      logUpdateDiscarded(key, metadata);
      assert metadata != null : "[IRAC] Metadata must not be null!";
      command.fail(); //this prevents the sending to the backup owners
      entry.setChanged(false); //this prevents the local node to apply the changes.
   }

   private void logUpdateDiscarded(Object key, IracMetadata metadata) {
      if (isTraceEnabled()) {
         getLog().tracef("[IRAC] Update from remote site discarded. Metadata=%s, key=%s", metadata, key);
      }
   }

   private IracMetadata getIracMetadata(CacheEntry<?, ?> entry) {
      PrivateMetadata privateMetadata = entry.getInternalMetadata();
      if (privateMetadata == null) { //new entry!
         return iracVersionGenerator.getTombstone(entry.getKey());
      }
      IracMetadata metadata = privateMetadata.iracMetadata();
      return metadata == null ? iracVersionGenerator.getTombstone(entry.getKey()) : metadata;
   }

}
