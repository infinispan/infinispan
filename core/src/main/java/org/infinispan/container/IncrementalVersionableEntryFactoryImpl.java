/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.container;

import org.infinispan.container.versioning.NonExistingVersion;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ClusteredRepeatableReadEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.factories.annotations.Start;
import org.infinispan.metadata.Metadatas;

/**
 * An entry factory that is capable of dealing with SimpleClusteredVersions.  This should <i>only</i> be used with
 * optimistically transactional, repeatable read, write skew check enabled caches in replicated or distributed mode.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class IncrementalVersionableEntryFactoryImpl extends EntryFactoryImpl {

   @Start (priority = 9)
   public void setWriteSkewCheckFlag() {
      localModeWriteSkewCheck = false;
      useRepeatableRead = true;
   }

   @Override
   protected MVCCEntry createWrappedEntry(Object key, CacheEntry cacheEntry, InvocationContext context,
                                          Metadata providedMetadata, boolean isForInsert, boolean forRemoval, boolean skipRead) {
      Metadata metadata;
      Object value;
      if (cacheEntry != null) {
         value = cacheEntry.getValue();
         Metadata entryMetadata = cacheEntry.getMetadata();
         if (providedMetadata != null && entryMetadata != null) {
            metadata = Metadatas.applyVersion(entryMetadata, providedMetadata);
         } else if (providedMetadata == null) {
            metadata = entryMetadata; // take the metadata in memory
         } else {
            metadata = providedMetadata;
         }
         if (context.isOriginLocal() && context.isInTxScope()) {
            ((TxInvocationContext) context).getCacheTransaction().addVersionRead(key, skipRead ? null : metadata.version());
         }
      } else {
         value = null;
         metadata = providedMetadata == null ? new EmbeddedMetadata.Builder().version(NonExistingVersion.INSTANCE).build()
               : providedMetadata;
         if (context.isOriginLocal() && context.isInTxScope()) {
            ((TxInvocationContext) context).getCacheTransaction().addVersionRead(key, skipRead ? null : NonExistingVersion.INSTANCE);
         }
      }

      MVCCEntry entry = new ClusteredRepeatableReadEntry(key, value, metadata);
      if (forRemoval) {
         entry.setRemoved(true);
      }
      return entry;
   }

}
