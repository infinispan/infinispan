/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA.
 */

package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ClusteredRepeatableReadEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.VersionedEntryWrappingInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;

/**
 * Wrapping Interceptor for Total Order protocol when versions are needed
 *
 * @author Mircea.Markus@jboss.com
 * @author Pedro Ruivo
 * @since 5.3
 */
public class TotalOrderVersionedEntryWrappingInterceptor extends VersionedEntryWrappingInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderVersionedEntryWrappingInterceptor.class);
   private static final EntryVersionsMap EMPTY_VERSION_MAP = new EntryVersionsMap();
   private boolean trace;

   @Start
   public void setLogLevel() {
      trace = log.isTraceEnabled();
   }

   @Override
   public final Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {

      if (ctx.isOriginLocal()) {
         //for local mode keys
         ctx.getCacheTransaction().setUpdatedEntryVersions(EMPTY_VERSION_MAP);
         Object retVal = invokeNextInterceptor(ctx, command);
         if (shouldCommitDuringPrepare(command, ctx)) {
            commitContextEntries(ctx, null, null);
         }
         return retVal;
      }

      //Remote context, delivered in total order

      wrapEntriesForPrepare(ctx, command);

      Object retVal = invokeNextInterceptor(ctx, command);

      EntryVersionsMap versionsMap = cdl.createNewVersionsAndCheckForWriteSkews(versionGenerator, ctx,
                                                                                (VersionedPrepareCommand) command);

      if (command.isOnePhaseCommit()) {
         commitContextEntries(ctx, null, null);
      } else {
         if (trace)
            log.tracef("Transaction %s will be committed in the 2nd phase", ctx.getGlobalTransaction().globalId());
      }

      return versionsMap == null ? retVal : new ArrayList<Object>(versionsMap.keySet());
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         commitContextEntries(ctx, null, null);
      }
   }

   @Override
   protected void commitContextEntry(CacheEntry entry, InvocationContext ctx, FlagAffectedCommand command, EntryVersion userProvidedVersion) {
      if (ctx.isInTxScope() && !isFromStateTransfer(ctx)) {
         ClusteredRepeatableReadEntry clusterMvccEntry = (ClusteredRepeatableReadEntry) entry;
         EntryVersion existingVersion = clusterMvccEntry.getVersion();

         EntryVersion newVersion;
         if (existingVersion == null) {
            newVersion = versionGenerator.generateNew();
         } else {
            newVersion = versionGenerator.increment((IncrementableEntryVersion) existingVersion);
         }
         cdl.commitEntry(entry, newVersion, command, ctx);
      } else {
         // This could be a state transfer call!
         cdl.commitEntry(entry, entry.getVersion(), command, ctx);
      }
   }
}
