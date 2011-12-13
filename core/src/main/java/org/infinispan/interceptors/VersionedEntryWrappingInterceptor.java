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

package org.infinispan.interceptors;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Interceptor in charge with wrapping entries and add them in caller's context.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public class VersionedEntryWrappingInterceptor extends EntryWrappingInterceptor {

   private VersionGenerator versionGenerator;
   private static final Log log = LogFactory.getLog(VersionedEntryWrappingInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void initialize(VersionGenerator versionGenerator) {
      this.versionGenerator = versionGenerator;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object retval = super.visitPrepareCommand(ctx, command);
      EntryVersionsMap newVersionData = cll.createNewVersionsAndCheckForWriteSkews(versionGenerator, ctx, (VersionedPrepareCommand) command);

      return newVersionData == null ? retval : newVersionData;
   }


   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      try {
         if (ctx.isOriginLocal())
            ((VersionedCommitCommand) command).setUpdatedVersions(ctx.getCacheTransaction().getUpdatedEntryVersions());

         return invokeNextInterceptor(ctx, command);
      } finally {
         if (!ctx.isOriginLocal())
            ctx.getCacheTransaction().setUpdatedEntryVersions(((VersionedCommitCommand) command).getUpdatedVersions());
         commitContextEntries(ctx);
      }
   }

   @Override
   protected void commitContextEntry(CacheEntry entry, InvocationContext ctx, boolean skipOwnershipCheck) {
      if (ctx.isInTxScope()) {
         EntryVersion version = ((TxInvocationContext) ctx).getCacheTransaction().getUpdatedEntryVersions().get(entry.getKey());
         cll.commitEntry(entry, version, skipOwnershipCheck);
      } else {
         // This could be a state transfer call!
         cll.commitEntry(entry, entry.getVersion(), skipOwnershipCheck);
      }
   }
}
