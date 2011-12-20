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
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Map;

/**
 * A version of the {@link DistributionInterceptor} that adds logic to handling prepares when entries are versioned.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class VersionedDistributionInterceptor extends DistributionInterceptor {

   private static final Log log = LogFactory.getLog(VersionedDistributionInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected PrepareCommand buildPrepareCommandForResend(TxInvocationContext ctx, CommitCommand commit) {
      // Make sure this is 1-Phase!!
      PrepareCommand command = cf.buildVersionedPrepareCommand(commit.getGlobalTransaction(), ctx.getModifications(), true);

      // Build a map of keys to versions as they were seen by the transaction originator's transaction context
      EntryVersionsMap vs = new EntryVersionsMap();
      for (CacheEntry ce: ctx.getLookedUpEntries().values()) {
         vs.put(ce.getKey(), (IncrementableEntryVersion) ce.getVersion());
      }

      // Make sure this version map is attached to the prepare command so that lock owners can perform write skew checks
      ((VersionedPrepareCommand) command).setVersionsSeen(vs);
      return command;
   }


   @Override
   protected void prepareOnAffectedNodes(TxInvocationContext ctx, PrepareCommand command, Collection<Address> recipients, boolean ignored) {

      // Build a map of keys to versions as they were seen by the transaction originator's transaction context
      EntryVersionsMap vs = new EntryVersionsMap();
      for (WriteCommand wc : command.getModifications()) {
         for (Object k : wc.getAffectedKeys()) {
            vs.put(k, (IncrementableEntryVersion) ctx.lookupEntry(k).getVersion());
         }
      }

      // Make sure this version map is attached to the prepare command so that lock owners can perform write skew checks
      ((VersionedPrepareCommand) command).setVersionsSeen(vs);

      // Perform the RPC
      Map<Address, Response> resps = rpcManager.invokeRemotely(recipients, command, true, true);

      // Now store newly generated versions from lock owners for use during the commit phase.
      CacheTransaction ct = ctx.getCacheTransaction();
      for (Response r : resps.values()) {
         if (r != null && r.isSuccessful()) {
            SuccessfulResponse sr = (SuccessfulResponse) r;
            EntryVersionsMap uv = (EntryVersionsMap) sr.getResponseValue();
            if (uv != null) ct.setUpdatedEntryVersions(uv.merge(ct.getUpdatedEntryVersions()));
         }
      }
   }
}
