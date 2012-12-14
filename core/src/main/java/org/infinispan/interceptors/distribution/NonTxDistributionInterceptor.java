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
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;
import java.util.Map;

/**
 * Handles the distribution of the non-transactional caches.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class NonTxDistributionInterceptor extends BaseDistributionInterceptor {

   private static Log log = LogFactory.getLog(NonTxDistributionInterceptor.class);

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      try {
         Object returnValue = invokeNextInterceptor(ctx, command);
         if (needsRemoteGet(ctx, command, returnValue == null)) {
            Object key = command.getKey();
            returnValue = shouldFetchFromRemote(ctx, key) ? remoteGet(ctx, key, command) : null;
         }
         return returnValue;
      } catch (SuspectException e) {
         // retry
         return visitGetKeyValueCommand(ctx, command);
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      SingleKeyRecipientGenerator skrg = new SingleKeyRecipientGenerator(command.getKey());
      return handleWriteCommand(ctx, command, skrg, command.hasFlag(Flag.PUT_FOR_STATE_TRANSFER), false);
   }

   /**
    * Don't forward in the case of clear commands, just acquire local locks and broadcast.
    */
   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (ctx.isOriginLocal() && !isLocalModeForced(command)) {
         rpcManager.broadcastRpcCommand(command, isSynchronous(command));
      }
      return invokeNextInterceptor(ctx, command);
   }

   protected Object handleWriteCommand(InvocationContext ctx, WriteCommand command, RecipientGenerator recipientGenerator, boolean skipRemoteGet, boolean skipL1Invalidation) throws Throwable {

      // if this is local mode then skip distributing
      if (isLocalModeForced(command)) {
         return invokeNextInterceptor(ctx, command);
      }

      if (!ctx.isOriginLocal()) {
         Object returnValue = invokeNextInterceptor(ctx, command);
         handleRemoteWrite(ctx, command, recipientGenerator, skipL1Invalidation, isSynchronous(command));
         return returnValue;
      } else {
         return handleLocalWrite(ctx, command, recipientGenerator, skipL1Invalidation, isSynchronous(command));
      }
   }

   protected Object handleLocalWrite(InvocationContext ctx, WriteCommand command, RecipientGenerator rg, boolean skipL1Invalidation, boolean sync) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      if (!isSingleOwnerAndLocal(rg)) {
         List<Address> recipients = rg.generateRecipients();
         if (recipients.contains(rpcManager.getAddress()) && (!command.isSuccessful())) {
            log.trace("Skipping remote invocation as the command hasn't executed correctly on owner");
         } else {
            Map<Address, Response> responseMap = rpcManager.invokeRemotely(recipients, command, sync);
            Address mainOwner;
            boolean equals = (mainOwner = recipients.get(0)).equals(rpcManager.getAddress());
            if (sync && !recipients.isEmpty() && !equals) {
               returnValue = getResponseFromPrimaryOwner(mainOwner, responseMap);
            }
         }
      }
      return returnValue;
   }

   protected void handleRemoteWrite(InvocationContext ctx, WriteCommand command, RecipientGenerator recipientGenerator, boolean skipL1Invalidation, boolean sync) throws Throwable {}

   private Object remoteGet(InvocationContext ctx, Object key, GetKeyValueCommand command) throws Throwable {
      if (trace) log.tracef("Doing a remote get for key %s", key);
      InternalCacheEntry ice = dm.retrieveFromRemoteSource(key, ctx, false, command);
      command.setRemotelyFetchedValue(ice);
      if (ice != null) {
         return ice.getValue();
      }
      return null;
   }

   protected Object getResponseFromPrimaryOwner(Address primaryOwner, Map<Address, Response> addressResponseMap) {
      if (addressResponseMap.isEmpty() || addressResponseMap.get(primaryOwner) == null) {
         return null;
      }

      Response fromPrimaryOwner = addressResponseMap.get(primaryOwner);
      if (!fromPrimaryOwner.isSuccessful()) {
         throw new CacheException("Got unsuccessful response" + fromPrimaryOwner);
      } else {
         return ((SuccessfulResponse) fromPrimaryOwner).getResponseValue();
      }
   }
}
