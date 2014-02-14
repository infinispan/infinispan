package org.infinispan.interceptors.distribution;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.RemoteValueRetrievedListener;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.ClusteringInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ClusteredGetResponseValidityFilter;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.rpc.RpcOptionsBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
   protected RemoteValueRetrievedListener rvrl;

   private static final Log log = LogFactory.getLog(BaseDistributionInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void injectDependencies(DistributionManager distributionManager, ClusteringDependentLogic cdl,
                                  RemoteValueRetrievedListener rvrl) {
      this.dm = distributionManager;
      this.cdl = cdl;
      this.rvrl = rvrl;
   }

   @Override
   protected final InternalCacheEntry retrieveFromRemoteSource(Object key, InvocationContext ctx, boolean acquireRemoteLock, FlagAffectedCommand command, boolean isWrite) throws Exception {
      GlobalTransaction gtx = ctx.isInTxScope() ? ((TxInvocationContext)ctx).getGlobalTransaction() : null;
      ClusteredGetCommand get = cf.buildClusteredGetCommand(key, command.getFlags(), acquireRemoteLock, gtx);
      get.setWrite(isWrite);


      RpcOptionsBuilder rpcOptionsBuilder = rpcManager.getRpcOptionsBuilder(ResponseMode.WAIT_FOR_VALID_RESPONSE, false);
      int lastTopologyId = -1;
      InternalCacheEntry value = null;
      while (value == null) {
         final CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
         final int currentTopologyId = cacheTopology.getTopologyId();

         if (trace) {
            log.tracef("Perform remote get for key %s. topologyId=%s, currentTopologyId=%s",
                       key, lastTopologyId, currentTopologyId);
         }
         List<Address> targets;
         if (lastTopologyId < currentTopologyId) {
            // Cache topology has changed or it is the first time.
            lastTopologyId = currentTopologyId;
            targets = new ArrayList<Address>(cacheTopology.getReadConsistentHash().locateOwners(key));
         } else if (lastTopologyId == currentTopologyId) {
            // Same topologyId, but the owners could have already installed the next topology
            // Lets try with write consistent owners (the read owners in the next topology)
            lastTopologyId = currentTopologyId + 1;
            targets = new ArrayList<Address>(cacheTopology.getWriteConsistentHash().locateOwners(key));
            // Remove already contacted nodes
            targets.removeAll(cacheTopology.getReadConsistentHash().locateOwners(key));
            if (targets.isEmpty()) {
               if (trace) {
                  log.tracef("No valid values found for key '%s' (topologyId=%s).", key, currentTopologyId);
               }
               break;
            }
         } else { // lastTopologyId > currentTopologyId
            // We have not received a valid value from the write CH owners either, and the topology id hasn't changed
            if (trace) {
               log.tracef("No valid values found for key '%s' (topologyId=%s).", key, currentTopologyId);
            }
            break;
         }

         value = invokeClusterGetCommandRemotely(targets, rpcOptionsBuilder, get, key);
         if (trace) {
            log.tracef("Remote get of key '%s' (topologyId=%s) returns %s", key, currentTopologyId, value);
         }
      }
      return value;
   }

   private InternalCacheEntry invokeClusterGetCommandRemotely(List<Address> targets, RpcOptionsBuilder rpcOptionsBuilder,
                                                      ClusteredGetCommand get, Object key) {
      ResponseFilter filter = new ClusteredGetResponseValidityFilter(targets, rpcManager.getAddress());
      RpcOptions options = rpcOptionsBuilder.responseFilter(filter).build();
      Map<Address, Response> responses = rpcManager.invokeRemotely(targets, get, options);

      if (!responses.isEmpty()) {
         for (Response r : responses.values()) {
            if (r instanceof SuccessfulResponse) {

               // The response value might be null.
               SuccessfulResponse response = (SuccessfulResponse) r;
               Object responseValue = response.getResponseValue();
               if (responseValue == null) {
                  continue;
               }

               InternalCacheValue cacheValue = (InternalCacheValue) responseValue;
               InternalCacheEntry ice = cacheValue.toInternalCacheEntry(key);
               if (rvrl != null) {
                  rvrl.remoteValueFound(ice);
               }
               return ice;
            }
         }
      }
      if (rvrl != null) {
         rvrl.remoteValueNotFound(key);
      }
      return null;
   }

   protected final Object handleNonTxWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      if (ctx.isInTxScope()) {
         throw new CacheException("Attempted execution of non-transactional write command in a transactional invocation context");
      }

      RecipientGenerator recipientGenerator = new SingleKeyRecipientGenerator(command.getKey());

      // see if we need to load values from remote sources first
      if (needValuesFromPreviousOwners(ctx, command)) {
         remoteGetBeforeWrite(ctx, command, recipientGenerator);
      }

      // invoke the command locally, we need to know if it's successful or not
      Object localResult = invokeNextInterceptor(ctx, command);

      // if this is local mode then skip distributing
      if (isLocalModeForced(command)) {
         return localResult;
      }


      boolean isSync = isSynchronous(command);
      Address primaryOwner = cdl.getPrimaryOwner(command.getKey());
      int commandTopologyId = command.getTopologyId();
      int currentTopologyId = stateTransferManager.getCacheTopology().getTopologyId();
      // TotalOrderStateTransferInterceptor doesn't set the topology id for PFERs.
      // TODO Shouldn't PFERs be executed in a tx with total order?
      boolean topologyChanged = isSync && currentTopologyId != commandTopologyId && commandTopologyId != -1;
      if (command.isSuccessful() && topologyChanged) {
         log.tracef("Cache topology changed while the command was executing: expected %d, got %d",
               commandTopologyId, currentTopologyId);
         throw new OutdatedTopologyException("Cache topology changed while the command was executing: expected " +
               commandTopologyId + ", got " + currentTopologyId);
      }

      ValueMatcher valueMatcher = command.getValueMatcher();
      if (!ctx.isOriginLocal()) {
         if (primaryOwner.equals(rpcManager.getAddress())) {
            if (!command.isSuccessful()) {
               log.tracef("Skipping the replication of the conditional command as it did not succeed on primary owner (%s).", command);
               return localResult;
            }
            List<Address> recipients = recipientGenerator.generateRecipients();
            // Ignore the previous value on the backup owners
            command.setValueMatcher(ValueMatcher.MATCH_ALWAYS);
            try {
               rpcManager.invokeRemotely(recipients, command, determineRpcOptionsForBackupReplication(rpcManager,
                                                                                                      isSync, recipients));
            } finally {
               // Switch to the retry policy, in case the primary owner changed and the write already succeeded on the new primary
               command.setValueMatcher(valueMatcher.matcherForRetry());
            }
         }
         return localResult;
      } else {
         if (primaryOwner.equals(rpcManager.getAddress())) {
            if (!command.isSuccessful()) {
               log.tracef("Skipping the replication of the command as it did not succeed on primary owner (%s).", command);
               return localResult;
            }
            List<Address> recipients = recipientGenerator.generateRecipients();
            log.tracef("I'm the primary owner, sending the command to all the backups (%s) in order to be applied.",
                  recipients);
            // check if a single owner has been configured and the target for the key is the local address
            boolean isSingleOwnerAndLocal = cacheConfiguration.clustering().hash().numOwners() == 1;
            if (!isSingleOwnerAndLocal) {
               // Ignore the previous value on the backup owners
               command.setValueMatcher(ValueMatcher.MATCH_ALWAYS);
               try {
                  rpcManager.invokeRemotely(recipients, command, determineRpcOptionsForBackupReplication(rpcManager,
                                                                                                         isSync, recipients));
               } finally {
                  // Switch to the retry policy, in case the primary owner changed and the write already succeeded on the new primary
                  command.setValueMatcher(valueMatcher.matcherForRetry());
               }
            }
            return localResult;
         } else {
            log.tracef("I'm not the primary owner, so sending the command to the primary owner(%s) in order to be forwarded", primaryOwner);
            boolean isSyncForwarding = isSync || isNeedReliableReturnValues(command);

            Map<Address, Response> addressResponseMap;
            try {
               addressResponseMap = rpcManager.invokeRemotely(Collections.singletonList(primaryOwner), command,
                     rpcManager.getDefaultRpcOptions(isSyncForwarding));
            } catch (RemoteException e) {
               Throwable ce = e;
               while (ce instanceof RemoteException) {
                  ce = ce.getCause();
               }
               if (ce instanceof OutdatedTopologyException) {
                  // If the primary owner throws an OutdatedTopologyException, it must be because the command succeeded there
                  if (trace) log.tracef("Changing the value matching policy from %s to %s (original value was %s)",
                        command.getValueMatcher(), valueMatcher.matcherForRetry(), valueMatcher);
                  command.setValueMatcher(valueMatcher.matcherForRetry());
               }
               throw e;
            } catch (SuspectException e) {
               // If the primary owner became suspected, we don't know if it was able to replicate it's data properly
               // to all backup owners and notify all listeners, thus we need to retry with new matcher in case if
               // it had updated the backup owners
               if (trace) log.tracef("Primary owner suspected - Changing the value matching policy from %s to %s " +
                                           "(original value was %s)", command.getValueMatcher(),
                                     valueMatcher.matcherForRetry(), valueMatcher);
               command.setValueMatcher(valueMatcher.matcherForRetry());
               throw e;
            }
            if (!isSyncForwarding) return localResult;

            Object primaryResult = getResponseFromPrimaryOwner(primaryOwner, addressResponseMap);
            command.updateStatusFromRemoteResponse(primaryResult);
            return primaryResult;
         }
      }
   }

   private RpcOptions determineRpcOptionsForBackupReplication(RpcManager rpc, boolean isSync, List<Address> recipients) {
      RpcOptions options;
      if (isSync) {
         // If no recipients, means a broadcast, so we can ignore leavers
         if (recipients == null) {
            options = rpc.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS).build();
         } else {
            options = rpc.getDefaultRpcOptions(true);
         }
      } else {
         options = rpc.getDefaultRpcOptions(false);
      }
      return options;
   }

   private Object getResponseFromPrimaryOwner(Address primaryOwner, Map<Address, Response> addressResponseMap) {
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

   /**
    * @return Whether a remote get is needed to obtain the previous values of the affected entries.
    */
   protected abstract boolean needValuesFromPreviousOwners(InvocationContext ctx, WriteCommand command);

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
         if (recipients == null) {
            recipients = cdl.getOwners(key);
         }
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
            recipients = cdl.getOwners(keys);
         }
         return recipients;
      }

      @Override
      public Collection<Object> getKeys() {
         return keys;
      }
   }
}
