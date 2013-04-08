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
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.responses.ExceptionResponse;
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
   private static final boolean trace = log.isTraceEnabled();

   private boolean useLockForwarding;

   @Start
   public void start() {
      useLockForwarding = cacheConfiguration.locking().supportsConcurrentUpdates();
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      try {
         Object returnValue = invokeNextInterceptor(ctx, command);
         if (returnValue == null) {
            Object key = command.getKey();
            if (needsRemoteGet(ctx, command)) {
               returnValue = remoteGet(ctx, key, command);
            }
            if (returnValue == null) {
               returnValue = localGet(ctx, key, false, command);
            }
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
         rpcManager.invokeRemotely(null, command, rpcManager.getDefaultRpcOptions(isSynchronous(command)));
      }
      return invokeNextInterceptor(ctx, command);
   }

   protected Object handleWriteCommand(InvocationContext ctx, WriteCommand command, RecipientGenerator recipientGenerator, boolean skipRemoteGet, boolean skipL1Invalidation) throws Throwable {
      // see if we need to load values from remote sources first
      remoteGetBeforeWrite(ctx, command, recipientGenerator);

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

   private void remoteGetBeforeWrite(InvocationContext ctx, WriteCommand command, KeyGenerator keygen) throws Throwable {
      // this should only happen if:
      //   a) unsafeUnreliableReturnValues is false
      //   b) unsafeUnreliableReturnValues is true, the command is conditional
      if (isNeedReliableReturnValues(command) || command.isConditional()) {
         for (Object k : keygen.getKeys()) {
            Object returnValue = remoteGetBeforeWrite(ctx, k, command);
            if (returnValue == null && (!useLockForwarding || cdl.localNodeIsPrimaryOwner(k))) {
               // We either did not go remotely (because the value should be local now) or we did but the remote value is null.
               // Then it makes sense to try a local get and wrap again. This will compensate the fact the the entry was not local
               // earlier when the EntryWrappingInterceptor executed during current invocation context but it should be now.
               localGet(ctx, k, true, command);
            }
         }
      }
   }

   private Object remoteGetBeforeWrite(InvocationContext ctx, Object key, FlagAffectedCommand command) throws Throwable {
      // During state transfer it is possible for an entry to map to the local node, but not have been brought locally yet
      // (not present in data container yet). In that case we fetch the value remotely first. If the value exists remotely (non-null)
      // then we use it. Otherwise if remote value is null it's possible that the state transfer finished in between
      // the "isAffectedByRehash" and "retrieveFromRemoteSource" so the caller can hope to find the value in the local data container.
      if (dm.isAffectedByRehash(key) && !dataContainer.containsKey(key)) {
         if (trace) log.tracef("Doing a remote get for key %s", key);

         // attempt a remote lookup
         InternalCacheEntry ice = retrieveFromRemoteSource(key, ctx, false, command);
         if (ice != null) {
            if (!ctx.replaceValue(key, ice)) {
               entryFactory.wrapEntryForPut(ctx, key, ice, false, command);
            }
            return ice.getValue();
         }
      } else {
         if (trace) log.tracef("Not doing a remote get for key %s since entry is not affected by rehash or is already in data container. We are %s, owners are %s", key, rpcManager.getAddress(), dm.locate(key));
      }
      return null;
   }

   private Object localGet(InvocationContext ctx, Object key, boolean isWrite, FlagAffectedCommand command) throws Throwable {
      InternalCacheEntry ice = dataContainer.get(key);
      if (ice != null) {
         if (!ctx.replaceValue(key, ice))  {
            if (isWrite)
               entryFactory.wrapEntryForPut(ctx, key, ice, false, command);
            else
               ctx.putLookedUpEntry(key, ice);
         }
         return command instanceof GetCacheEntryCommand ? ice : ice.getValue();
      }
      return null;
   }

   protected Object handleLocalWrite(InvocationContext ctx, WriteCommand command, RecipientGenerator rg, boolean skipL1Invalidation, boolean sync) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      if (!isSingleOwnerAndLocal(rg)) {
         List<Address> recipients = rg.generateRecipients();
         if (!command.isSuccessful() && recipients.contains(rpcManager.getAddress())) {
            log.tracef("Skipping remote invocation as the command hasn't executed correctly on owner %s", rpcManager.getAddress());
         } else {
            Map<Address, Response> responseMap = rpcManager.invokeRemotely(recipients, command, rpcManager.getDefaultRpcOptions(sync));
            if (sync && !recipients.isEmpty()) {
               Address primaryOwner = recipients.get(0);
               if (!primaryOwner.equals(rpcManager.getAddress())) {
                  returnValue = getResponseFromPrimaryOwner(primaryOwner, responseMap);
               }
            }
         }
      }
      return returnValue;
   }

   protected void handleRemoteWrite(InvocationContext ctx, WriteCommand command, RecipientGenerator recipientGenerator, boolean skipL1Invalidation, boolean sync) throws Throwable {}

   private Object remoteGet(InvocationContext ctx, Object key, GetKeyValueCommand command) throws Throwable {
      if (trace) log.tracef("Doing a remote get for key %s", key);
      InternalCacheEntry ice = retrieveFromRemoteSource(key, ctx, false, command);
      command.setRemotelyFetchedValue(ice);
      if (ice != null) {
         return ice.getValue();
      }
      return null;
   }

   protected Object getResponseFromPrimaryOwner(Address primaryOwner, Map<Address, Response> addressResponseMap) {
      Response fromPrimaryOwner = addressResponseMap.get(primaryOwner);
      if (fromPrimaryOwner == null) {
         log.tracef("Primary owner %s returned null", primaryOwner);
         return null;
      }
      if (!fromPrimaryOwner.isSuccessful()) {
         Throwable cause = fromPrimaryOwner instanceof ExceptionResponse ? ((ExceptionResponse)fromPrimaryOwner).getException() : null;
         throw new CacheException("Got unsuccessful response from primary owner: " + fromPrimaryOwner, cause);
      } else {
         return ((SuccessfulResponse) fromPrimaryOwner).getResponseValue();
      }
   }
}
