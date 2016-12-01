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

import org.infinispan.CacheException;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.OutdatedTopologyException;
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
   private static boolean trace = log.isTraceEnabled();

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         Set<Address> primaryOwners = new HashSet<Address>(command.getAffectedKeys().size());
         for (Object k : command.getAffectedKeys()) {
            primaryOwners.add(cdl.getPrimaryOwner(k));
         }
         primaryOwners.remove(rpcManager.getAddress());
         if (!primaryOwners.isEmpty()) {
            rpcManager.invokeRemotely(primaryOwners, command, isSynchronous(command));
         }
      } else {
         if (!command.isForwarded()) {
            //I need to forward this to all the nodes that are secondary owners
            Set<Object> keysIOwn = new HashSet<Object>(command.getAffectedKeys());
            for (Object k : command.getAffectedKeys())
               if (cdl.localNodeIsPrimaryOwner(k)) {
                  keysIOwn.add(k);
               }
            Collection<Address> backupOwners = cdl.getOwners(keysIOwn);
            if (!backupOwners.isEmpty()) {
               command.setFlags(Flag.SKIP_LOCKING);
               command.setForwarded(true);
               rpcManager.invokeRemotely(backupOwners, command, isSynchronous(command));
               command.setForwarded(false);
            }
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
         if (command.isSuccessful()) {
            checkForOutdatedTopology(command);

            if (!isSingleOwnerAndLocal(rg)) {
               command.setIgnorePreviousValue(true);
               rpcManager.invokeRemotely(recipients, command, sync);
            }
         }
         return result;
      } else {
         log.tracef("I'm not the primary owner, so sending the command to the primary owner(%s) in order to be forwarded", primaryOwner);
         Object localResult = invokeNextInterceptor(ctx, command);
         checkForOutdatedTopology(command);

         Map<Address, Response> addressResponseMap;
         try {
            addressResponseMap = rpcManager.invokeRemotely(Collections.singletonList(primaryOwner), command, sync);
         } catch (RemoteException e) {
            Throwable ce = e;
            while (ce instanceof RemoteException) {
               ce = ce.getCause();
            }
            if (ce instanceof OutdatedTopologyException) {
               // TODO Set another flag that will make the new primary owner only ignore the final value of the command
               // If the primary owner throws an OutdatedTopologyException, it must be because the command succeeded there
               command.setIgnorePreviousValue(true);
            }
            throw e;
         } catch (SuspectException e) {
            // If the primary owner became suspected, we don't know if it was able to replicate it's data properly
            // to all backup owners and notify all listeners, thus we need to retry with new matcher in case if
            // it had updated the backup owners
            if (trace) log.trace("Primary owner suspected - retrying and ignoring the previous value");
            command.setIgnorePreviousValue(true);
            throw e;
         }

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
         if (command.isSuccessful()) {
            checkForOutdatedTopology(command);
            if (primaryOwner.equals(rpcManager.getAddress())) {
               command.setIgnorePreviousValue(true);
               rpcManager.invokeRemotely(recipientGenerator.generateRecipients(), command, sync);
            }
         }
      }
   }

   protected Object getResponseFromPrimaryOwner(Address primaryOwner, Map<Address, Response> addressResponseMap) {
      Response fromPrimaryOwner = addressResponseMap.get(primaryOwner);
      if (fromPrimaryOwner == null) {
         log.tracef("Primary owner %s returned null", primaryOwner);
         return null;
      }
      if (fromPrimaryOwner.isSuccessful()) {
         return ((SuccessfulResponse) fromPrimaryOwner).getResponseValue();
      }

      if (addressResponseMap.get(primaryOwner) instanceof CacheNotFoundResponse) {
         // This means the cache wasn't running on the primary owner, so the command wasn't executed.
         // We throw an OutdatedTopologyException, StateTransferInterceptor will catch the exception and
         // it will then retry the command.
         throw new OutdatedTopologyException("Cache is no longer running on primary owner " + primaryOwner);
      }

      Throwable cause = fromPrimaryOwner instanceof ExceptionResponse ? ((ExceptionResponse)fromPrimaryOwner).getException() : null;
      throw new CacheException("Got unsuccessful response from primary owner: " + fromPrimaryOwner, cause);
   }
}
