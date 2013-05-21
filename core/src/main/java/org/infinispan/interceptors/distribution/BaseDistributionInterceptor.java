/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.interceptors.distribution;

import org.infinispan.CacheException;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.ClusteringInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.responses.ClusteredGetResponseValidityFilter;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Immutables;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.*;

/**
 * Base class for distribution of entries across a cluster.
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @author Pete Muir
 * @author Dan Berindei <dan@infinispan.org>
 * @since 4.0
 */
public abstract class BaseDistributionInterceptor extends ClusteringInterceptor {

   protected DistributionManager dm;

   protected ClusteringDependentLogic cdl;

   private static final Log log = LogFactory.getLog(BaseDistributionInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void injectDependencies(DistributionManager distributionManager, ClusteringDependentLogic cdl) {
      this.dm = distributionManager;
      this.cdl = cdl;
   }

   @Override
   protected final InternalCacheEntry retrieveFromRemoteSource(Object key, InvocationContext ctx, boolean acquireRemoteLock, FlagAffectedCommand command) throws Exception {
      GlobalTransaction gtx = acquireRemoteLock ? ((TxInvocationContext)ctx).getGlobalTransaction() : null;
      ClusteredGetCommand get = cf.buildClusteredGetCommand(key, command.getFlags(), acquireRemoteLock, gtx);

      List<Address> targets = new ArrayList<Address>(stateTransferManager.getCacheTopology().getReadConsistentHash().locateOwners(key));
      // if any of the recipients has left the cluster since the command was issued, just don't wait for its response
      targets.retainAll(rpcManager.getTransport().getMembers());
      ResponseFilter filter = new ClusteredGetResponseValidityFilter(targets, rpcManager.getAddress());
      RpcOptions options = rpcManager.getRpcOptionsBuilder(ResponseMode.WAIT_FOR_VALID_RESPONSE, false)
            .responseFilter(filter).build();
      Map<Address, Response> responses = rpcManager.invokeRemotely(targets, get, options);

      if (!responses.isEmpty()) {
         for (Response r : responses.values()) {
            if (r instanceof SuccessfulResponse) {
               InternalCacheValue cacheValue = (InternalCacheValue) ((SuccessfulResponse) r).getResponseValue();
               return cacheValue.toInternalCacheEntry(key);
            }
         }
      }

      // TODO If everyone returned null, and the read CH has changed, retry the remote get.
      // Otherwise our get command might be processed by the old owners after they have invalidated their data
      // and we'd return a null even though the key exists on
      return null;
   }

   protected final Object handleNonTxWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      if (ctx.isInTxScope()) {
         throw new CacheException("Attempted execution of non-transactional write command in a transactional invocation context");
      }

      RecipientGenerator recipientGenerator = new SingleKeyRecipientGenerator(command.getKey());

      // see if we need to load values from remote sources first
      remoteGetBeforeWrite(ctx, command, recipientGenerator);

      // if this is local mode then skip distributing
      if (isLocalModeForced(command)) {
         return invokeNextInterceptor(ctx, command);
      }

      boolean isSync = isSynchronous(command);
      if (!ctx.isOriginLocal()) {
         Object returnValue = invokeNextInterceptor(ctx, command);
         Address primaryOwner = cdl.getPrimaryOwner(command.getKey());
         if (primaryOwner.equals(rpcManager.getAddress())) {
            rpcManager.invokeRemotely(recipientGenerator.generateRecipients(), command, rpcManager.getDefaultRpcOptions(isSync));
         }
         return returnValue;
      } else {
         Address primaryOwner = cdl.getPrimaryOwner(command.getKey());
         if (primaryOwner.equals(rpcManager.getAddress())) {
            List<Address> recipients = recipientGenerator.generateRecipients();
            log.tracef("I'm the primary owner, sending the command to all (%s) the recipients in order to be applied.", recipients);
            Object result = invokeNextInterceptor(ctx, command);
            // check if a single owner has been configured and the target for the key is the local address
            boolean isSingleOwnerAndLocal = cacheConfiguration.clustering().hash().numOwners() == 1
                  && recipients != null
                  && recipients.size() == 1
                  && recipients.get(0).equals(rpcManager.getTransport().getAddress());
            if (!isSingleOwnerAndLocal) {
               rpcManager.invokeRemotely(recipients, command, rpcManager.getDefaultRpcOptions(isSync));
            }
            return result;
         } else {
            log.tracef("I'm not the primary owner, so sending the command to the primary owner(%s) in order to be forwarded", primaryOwner);
            Object localResult = invokeNextInterceptor(ctx, command);
            Map<Address, Response> addressResponseMap = rpcManager.invokeRemotely(Collections.singletonList(primaryOwner), command,
                  rpcManager.getDefaultRpcOptions(isSync));
            //the remote node always returns the correct result, but if we're async, then our best option is the local
            //node. That might be incorrect though.
            if (!isSync) return localResult;

            return getResponseFromPrimaryOwner(primaryOwner, addressResponseMap);
         }
      }
   }

   private Object getResponseFromPrimaryOwner(Address primaryOwner, Map<Address, Response> addressResponseMap) {
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

   protected abstract void remoteGetBeforeWrite(InvocationContext ctx, WriteCommand command, RecipientGenerator keygen) throws Throwable;

   interface RecipientGenerator {

      Collection<Object> getKeys();

      List<Address> generateRecipients();
   }

   class SingleKeyRecipientGenerator implements RecipientGenerator {
      private final Object key;
      private final Set<Object> keys;
      private List<Address> recipients = null;

      SingleKeyRecipientGenerator(Object key) {
         this.key = key;
         keys = Collections.singleton(key);
      }

      @Override
      public List<Address> generateRecipients() {
         if (recipients == null) recipients = dm.locate(key);
         return recipients;
      }

      @Override
      public Collection<Object> getKeys() {
         return keys;
      }
   }

   class MultipleKeysRecipientGenerator implements RecipientGenerator {

      private final Collection<Object> keys;
      private List<Address> recipients = null;

      MultipleKeysRecipientGenerator(Collection<Object> keys) {
         this.keys = keys;
      }

      @Override
      public List<Address> generateRecipients() {
         if (recipients == null) {
            Set<Address> addresses = dm.locateAll(keys);
            recipients = Immutables.immutableListConvert(addresses);
         }
         return recipients;
      }

      @Override
      public Collection<Object> getKeys() {
         return keys;
      }
   }
}
