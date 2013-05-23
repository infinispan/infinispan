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

package org.infinispan.container.entries;

import org.infinispan.metadata.Metadata;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.versioned.Versioned;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A version of RepeatableReadEntry that can perform write-skew checks during prepare.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class ClusteredRepeatableReadEntry extends RepeatableReadEntry implements Versioned {

   private static final Log log = LogFactory.getLog(ClusteredRepeatableReadEntry.class);

   public ClusteredRepeatableReadEntry(Object key, Object value, Metadata metadata) {
      super(key, value, metadata);
   }

   public boolean performWriteSkewCheck(DataContainer container, TxInvocationContext ctx, boolean previousRead) {
      EntryVersion prevVersion;
      InternalCacheEntry ice = container.get(key);
      EntryVersion version = metadata.version();
      if (ice == null) {
         log.tracef("No entry for key %s found in data container" , key);
         prevVersion = ctx.getCacheTransaction().getLookedUpRemoteVersion(key);
         if (prevVersion == null) {
            log.tracef("No looked up remote version for key %s found in context" , key);
            return version == null;
         }
      } else {
         prevVersion = ice.getMetadata().version();
         if (prevVersion == null)
            throw new IllegalStateException("Entries cannot have null versions!");
      }
      if (log.isTraceEnabled()) {
         log.tracef("Is going to compare versions %s and %s for key %s. Was previously read? %s", prevVersion, version, key, previousRead);
      }
      // Could be that we didn't do a remote get first ... so we haven't effectively read this entry yet.
      if (version == null) return !previousRead;
      log.tracef("Comparing versions %s and %s for key %s: %s", prevVersion, version, key, prevVersion.compareTo(version));
      return InequalVersionComparisonResult.AFTER != prevVersion.compareTo(version);
   }

   // This entry is only used when versioning is enabled, and in these
   // situations, versions are generated internally and assigned at a
   // different stage to the rest of metadata. So, keep the versioned API
   // to make it easy to apply version information when needed.

   @Override
   public EntryVersion getVersion() {
      return metadata.version();
   }

   @Override
   public void setVersion(EntryVersion version) {
      metadata = metadata.builder().version(version).build();
   }

}
