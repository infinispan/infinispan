package org.infinispan.container;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ClusteredRepeatableReadEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;

/**
 * An entry factory that is capable of dealing with SimpleClusteredVersions.  This should <i>only</i> be used with
 * optimistically transactional, repeatable read, write skew check enabled caches in replicated or distributed mode.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class IncrementalVersionableEntryFactoryImpl extends EntryFactoryImpl {

   private VersionGenerator versionGenerator;

   @Inject
   public void injectVersionGenerator(VersionGenerator versionGenerator) {
      this.versionGenerator = versionGenerator;
   }

   @Override
   protected MVCCEntry createWrappedEntry(Object key, CacheEntry cacheEntry, InvocationContext context,
                                          boolean skipRead) {
      Metadata metadata = null;
      Object value = null;
      if (cacheEntry != null) {
         value = cacheEntry.getValue();
         metadata = cacheEntry.getMetadata(); // take the metadata in memory
      }
      if (metadata == null) {
         metadata = new EmbeddedMetadata.Builder().version(versionGenerator.nonExistingVersion()).build();
      }

      if (!skipRead) {
         addReadVersion(key, cacheEntry, context, metadata);
      }

      //only the ClusteredRepeatableReadEntry are used, even to represent the null values.
      ClusteredRepeatableReadEntry mvccEntry = new ClusteredRepeatableReadEntry(key, value, metadata);
      mvccEntry.setSkipLookup(cacheEntry == null ? false : cacheEntry.skipLookup());
      return mvccEntry;
   }

   @Override
   public boolean wrapExternalEntry(InvocationContext ctx, Object key, CacheEntry externalEntry, boolean isWrite,
                                    boolean skipRead) {
      boolean added = super.wrapExternalEntry(ctx, key, externalEntry, isWrite, skipRead);
      // TODO: if super.wrapExternalEntry calls createWrappedEntry we set the seen version twice
      if (added && ctx.isInTxScope() && !skipRead) {
         Metadata metadata = externalEntry == null ? null : externalEntry.getMetadata();
         EntryVersion version = metadata == null ? versionGenerator.nonExistingVersion() : metadata.version();
         ((TxInvocationContext) ctx).getCacheTransaction().replaceVersionRead(key, version);
      }
      return added;
   }

   private void addReadVersion(Object key, CacheEntry cacheEntry, InvocationContext context,
                               Metadata metadata) {
      if (context.isInTxScope()) {
         // Difficulties appear here when wrapping entry for write and the entry is not in DC but in cache store.
         // The actual read version should be set by persistence so we have to replace the version on the call
         // from cache loader. Callers have to make sure that this is not called for repeatable reads.
         EntryVersion version;
         if (cacheEntry != null) {
            version = metadata.version();
         } else {
            version = versionGenerator.nonExistingVersion();
         }
         ((TxInvocationContext) context).getCacheTransaction().replaceVersionRead(key, version);
      }
   }
}
