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

import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Interceptor in charge with wrapping entries and add them in caller's context.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public class VersionedEntryWrappingInterceptor extends EntryWrappingInterceptor {

   protected VersionGenerator versionGenerator;
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
      wrapEntriesForPrepare(ctx, command);
      EntryVersionsMap newVersionData= null;
      if (ctx.isOriginLocal() && !((LocalTransaction)ctx.getCacheTransaction()).isFromStateTransfer()) newVersionData = cdl.createNewVersionsAndCheckForWriteSkews(versionGenerator, ctx, (VersionedPrepareCommand) command);

      Object retval = invokeNextInterceptor(ctx, command);

      if (!ctx.isOriginLocal()) newVersionData = cdl.createNewVersionsAndCheckForWriteSkews(versionGenerator, ctx, (VersionedPrepareCommand) command);
      if (command.isOnePhaseCommit()) ctx.getCacheTransaction().setUpdatedEntryVersions(((VersionedPrepareCommand) command).getVersionsSeen());

      if (newVersionData != null) retval = newVersionData;
      if (command.isOnePhaseCommit()) commitContextEntries(ctx, null, null);
      return retval;
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
         commitContextEntries(ctx, null, null);
      }
   }

   @Override
   protected void commitContextEntry(CacheEntry entry, InvocationContext ctx, FlagAffectedCommand command, Metadata metadata) {
      if (ctx.isInTxScope() && !isFromStateTransfer(ctx)) {
         EntryVersion updatedEntryVersion = ((TxInvocationContext) ctx)
               .getCacheTransaction().getUpdatedEntryVersions().get(entry.getKey());
         Metadata commitMetadata;
         if (metadata == null)
            commitMetadata = new EmbeddedMetadata.Builder().version(updatedEntryVersion).build();
         else
            commitMetadata = metadata.builder().version(updatedEntryVersion).build();

         cdl.commitEntry(entry, commitMetadata, command, ctx);
      } else {
         // This could be a state transfer call!
         cdl.commitEntry(entry, entry.getMetadata(), command, ctx);
      }
   }

   @Override
   protected void checkIfKeyRead(InvocationContext context, Object key, VisitableCommand command) {
      if (command instanceof AbstractDataWriteCommand) {
         AbstractDataWriteCommand writeCommand = (AbstractDataWriteCommand) command;
         //keep track is only need in a clustered and transactional environment to perform the write skew check
         if (context.isInTxScope() && context.isOriginLocal()) {
            TxInvocationContext txInvocationContext = (TxInvocationContext) context;
            if (!writeCommand.hasFlag(Flag.PUT_FOR_STATE_TRANSFER) && writeCommand.isConditional() || 
                  !writeCommand.hasFlag(Flag.IGNORE_RETURN_VALUES)) {
               //State transfer does not show the old value for the application neither with the IGNORE_RETURN_VALUES.
               //on other hand, the conditional always read key!
               txInvocationContext.getCacheTransaction().addReadKey(key);
            }
            writeCommand.setPreviousRead(txInvocationContext.getCacheTransaction().keyRead(key));
         }
      } else if (command instanceof GetKeyValueCommand) {
         if (context.isInTxScope() && context.isOriginLocal()) {
            //always show the value to the application
            TxInvocationContext txInvocationContext = (TxInvocationContext) context;
            txInvocationContext.getCacheTransaction().addReadKey(key);
         }
      }
   }
}
