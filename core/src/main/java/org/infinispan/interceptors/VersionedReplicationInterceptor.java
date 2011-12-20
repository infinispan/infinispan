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

import java.util.Map;

/**
 * A form of the {@link ReplicationInterceptor} that adds additional logic to how prepares are handled.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class VersionedReplicationInterceptor extends ReplicationInterceptor {

   private static final Log log = LogFactory.getLog(VersionedReplicationInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected PrepareCommand buildPrepareCommandForResend(TxInvocationContext ctx, CommitCommand commit) {
      // Make sure this is 1-Phase!!
      PrepareCommand command = cf.buildVersionedPrepareCommand(commit.getGlobalTransaction(), ctx.getModifications(), true);

       super.buildPrepareCommandForResend(ctx, commit);
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
   protected void broadcastPrepare(TxInvocationContext context, PrepareCommand command) {
      // The additional logic here is that we need to wait for a prepare command to complete on the coordinator
      // since the coordinator provides updated version information in its response.  This updated version information
      // is then stored in the transactional context to be used during the commit phase.
      // However if the current node is already the coordinator, then we fall back to "normal" ReplicationInterceptor
      // logic for this step.
      if (!rpcManager.getTransport().isCoordinator()) {
         Map<Address, Response> resps = rpcManager.invokeRemotely(null, command, true, true);
         Response r = resps.get(rpcManager.getTransport().getCoordinator());  // We only really care about the coordinator's response.
         CacheTransaction ct = context.getCacheTransaction();
         if (r.isSuccessful()) {
            SuccessfulResponse sr = (SuccessfulResponse) r;
            EntryVersionsMap uv = (EntryVersionsMap) sr.getResponseValue();
            ct.setUpdatedEntryVersions(uv);
         }
      } else {
         super.broadcastPrepare(context, command);
      }
   }
}
