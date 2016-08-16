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
      Metadata metadata;
      Object value;
      if (cacheEntry != null) {
         value = cacheEntry.getValue();
         metadata = cacheEntry.getMetadata(); // take the metadata in memory
      } else {
         value = null;
         metadata = new EmbeddedMetadata.Builder().version(versionGenerator.nonExistingVersion()).build();
      }

      if (!skipRead) {
         addReadVersion(key, cacheEntry, context, metadata);
      }

      //only the ClusteredRepeatableReadEntry are used, even to represent the null values.
      return new ClusteredRepeatableReadEntry(key, value, metadata);
   }

   @Override
   public boolean wrapExternalEntry(InvocationContext ctx, Object key, CacheEntry externalEntry, Wrap wrap,
                                    boolean skipRead) {
      boolean added = super.wrapExternalEntry(ctx, key, externalEntry, wrap, skipRead);
      if (added) {
         if (ctx.isOriginLocal() && ctx.isInTxScope()) {
            if (externalEntry != null) {
               EntryVersion version = externalEntry.getMetadata().version();
               ((TxInvocationContext) ctx).getCacheTransaction().replaceVersionRead(key, version);
            }
         }
      }
      return added;
   }

   private void addReadVersion(Object key, CacheEntry cacheEntry, InvocationContext context,
                               Metadata metadata) {
      if (context.isOriginLocal() && context.isInTxScope()) {
         EntryVersion version;
         if (cacheEntry != null)
            version = metadata.version();
         else {
            version = versionGenerator.nonExistingVersion();
         }
         ((TxInvocationContext) context).getCacheTransaction().addVersionRead(key, version);
      }
   }
}
