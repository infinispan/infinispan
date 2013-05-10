/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.interceptors.distribution;

import org.infinispan.commands.DataCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Non-transactional interceptor to be used by caches that want to support concurrent writes.
 * It is implemented based on lock forwarding. E.g.
 * - 'k' is written on node A, owners(k)={B,C}
 * - A forwards the given command to B
 * - B acquires a lock on 'k' then it forwards it to the remaining owners: C
 * - C applies the change and returns to B (no lock acquisition is needed)
 * - B applies the result as well, releases the lock and returns the result of the operation to A.
 *
 * Note that even though this introduces an additional RPC (the forwarding), it behaves very well in conjunction with
 * consistent-hash aware hotrod clients which connect directly to the lock owner.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class NonTxConcurrentDistributionInterceptor extends NonTxDistributionInterceptor {

   private static Log log = LogFactory.getLog(NonTxConcurrentDistributionInterceptor.class);

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         Set<Address> primaryOwners = new HashSet<Address>(command.getAffectedKeys().size());
         for (Object k : command.getAffectedKeys()) {
            primaryOwners.add(cdl.getPrimaryOwner(k));
         }
         primaryOwners.remove(rpcManager.getAddress());
         if (!primaryOwners.isEmpty()) {
            rpcManager.invokeRemotely(primaryOwners, command, rpcManager.getDefaultRpcOptions(isSynchronous(command)));
         }
      }

      if (!command.isForwarded()) {
         //I need to forward this to all the nodes that are secondary owners
         Set<Object> keysIOwn = new HashSet<Object>(command.getAffectedKeys().size());
         for (Object k : command.getAffectedKeys()) {
            if (cdl.localNodeIsPrimaryOwner(k)) {
               keysIOwn.add(k);
            }
         }
         Collection<Address> backupOwners = cdl.getOwners(keysIOwn);
         if (!backupOwners.isEmpty()) {
            command.setFlags(Flag.SKIP_LOCKING);
            command.setForwarded(true);
            rpcManager.invokeRemotely(backupOwners, command, rpcManager.getDefaultRpcOptions(isSynchronous(command)));
            command.setForwarded(false);
         }
      }

      return invokeNextInterceptor(ctx, command);
   }

   protected Object handleLocalWrite(InvocationContext ctx, WriteCommand command, RecipientGenerator rg, boolean skipL1Invalidation, boolean sync) throws Throwable {
      Object key = ((DataCommand) command).getKey();
      Address primaryOwner = cdl.getPrimaryOwner(key);
      if (primaryOwner.equals(rpcManager.getAddress())) {
         List<Address> recipients = rg.generateRecipients();
         log.tracef("I'm the primary owner, sending the command to all (%s) the recipients in order to be applied.", recipients);
         Object result = invokeNextInterceptor(ctx, command);
         if (!isSingleOwnerAndLocal(rg)) {
            rpcManager.invokeRemotely(recipients, command, rpcManager.getDefaultRpcOptions(sync));
         }
         return result;
      } else {
         log.tracef("I'm not the primary owner, so sending the command to the primary owner(%s) in order to be forwarded", primaryOwner);
         Object localResult = invokeNextInterceptor(ctx, command);
         Map<Address, Response> addressResponseMap = rpcManager.invokeRemotely(Collections.singletonList(primaryOwner), command,
                                                                               rpcManager.getDefaultRpcOptions(sync));
         //the remote node always returns the correct result, but if we're async, then our best option is the local
         //node. That might be incorrect though.
         if (!sync) return localResult;

         return getResponseFromPrimaryOwner(primaryOwner, addressResponseMap);
      }
   }

   protected void handleRemoteWrite(InvocationContext ctx, WriteCommand command, RecipientGenerator recipientGenerator, boolean skipL1Invalidation, boolean sync) throws Throwable {
      if (command instanceof DataCommand) {
         DataCommand dataCommand = (DataCommand) command;
         Address primaryOwner = cdl.getPrimaryOwner(dataCommand.getKey());
         if (primaryOwner.equals(rpcManager.getAddress())) {
            rpcManager.invokeRemotely(recipientGenerator.generateRecipients(), command, rpcManager.getDefaultRpcOptions(sync));
         }
      }
   }
}
