package org.infinispan.distribution;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.RequestInvalidateL1Command;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.config.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.AggregatingNotifyingFutureImpl;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class L1ManagerImpl implements L1Manager {
	
	private final Log log = LogFactory.getLog(L1ManagerImpl.class);
	private final boolean trace = log.isTraceEnabled();
	
	private RpcManager rpcManager;
	private CommandsFactory commandsFactory;
	private DistributionManager distributionManager;
	private int threshold;

	private final ConcurrentMap<Object, Collection<Address>> requestors;
	
	public L1ManagerImpl() {
	   requestors = new ConcurrentHashMap<Object, Collection<Address>>();
   }
	
   @Inject
   public void init(Configuration configuration, RpcManager rpcManager, CommandsFactory commandsFactory, DistributionManager distributionManager) {
   	this.rpcManager = rpcManager;
   	this.commandsFactory = commandsFactory;
   	this.distributionManager = distributionManager;
   	this.threshold = configuration.getL1Threshold();
   }
   
   public void addRequestor(Object key, Address origin) {
   	synchronized (key) {
      	if (!requestors.containsKey(key)) {
      		requestors.put(key, new HashSet<Address>());
      	}
      	if (trace) log.trace("Key %s will be L1 cached by requestor %s so storing requestor for later invalidation", key, origin);
      	requestors.get(key).add(origin);
   	}
   }
   
   public NotifyingNotifiableFuture<Object> flushCache(int numCallRecipients, Collection<Object> keys, Object retval) {
   	if (rpcManager.getTransport().getMembers().size() > numCallRecipients) {
         if (trace) log.trace("Invalidating L1 caches for keys %s", keys);
         
         NotifyingNotifiableFuture<Object> future = new AggregatingNotifyingFutureImpl(retval, 2);
   	
         // notify all owners of the key to request flush their cache requestors
         Collection<Address> owners = distributionManager.getAffectedNodes(keys);
         Collection<Address> invalidationAddresses = buildInvalidationAddressList(keys);
         
         int nodes = owners.size() + invalidationAddresses.size();
         
         boolean multicast = isUseMulticast(nodes);
         
         if (trace) log.trace("There are %s nodes involved in invalidation. Threshold is: %s; using multicast: %s", nodes, threshold, multicast);
         
         if (multicast) {
         	if (trace) log.trace("Invalidating keys %s via multicast", keys);
         	InvalidateCommand ic = commandsFactory.buildInvalidateFromL1Command(false, keys);
         	rpcManager.broadcastRpcCommandInFuture(ic, future);
         } else {
         	RequestInvalidateL1Command ic = commandsFactory.buildRequestInvalidateL1Command(keys);
         	if (trace) log.trace("Invalidating keys %s via unicast", keys);
            if (trace) log.trace("Requesting %s to issue L1 invalidations", owners);
            rpcManager.invokeRemotelyInFuture(owners, ic, future);
            
            // now, flush ours
            flushLocalCache(keys, invalidationAddresses, future);
         }
         
         return future;
      } else {
         if (trace) log.trace("Not performing invalidation! numCallRecipients=%s", numCallRecipients);
      }
      return null;
   }
   
   
   private NotifyingNotifiableFuture<Object> flushLocalCache(Collection<Object> keys, Collection<Address> invalidationAddresses, NotifyingNotifiableFuture<Object> future) {
   	InvalidateCommand ic = commandsFactory.buildInvalidateFromL1Command(false, keys);
   	
      // Ask the caches who have requested from us to remove
      if (trace) log.trace("Keys %s needs invalidation on %s", keys, invalidationAddresses);
      rpcManager.invokeRemotelyInFuture(invalidationAddresses, ic, future);
      return future;
   }
   
   public NotifyingNotifiableFuture<Object> flushLocalCache(Collection<Object> keys) {
   	NotifyingNotifiableFuture<Object> future = new AggregatingNotifyingFutureImpl(null, 2);
   	Collection<Address> invalidationAddresses = buildInvalidationAddressList(keys);
   	return flushLocalCache(keys, invalidationAddresses, future);
   }
   
   private Collection<Address> buildInvalidationAddressList(Collection<Object> keys) {
   	Collection<Address> addresses = new HashSet<Address>();
   	for (Object key : keys) {
   		synchronized (key) {
      		if (requestors.containsKey(key)) {
      			addresses.addAll(requestors.get(key));
      		}
   		}
   	}
   	return addresses;
   }
   
   private boolean isUseMulticast(int nodes) {
   	// User has requested unicast or multicast only
   	if (threshold == -1) return false;
   	if (threshold == 0) return true;
   	// Underlying transport is not multicast capable
   	if (!rpcManager.getTransport().isMulticastCapable()) return false;
   	return nodes > threshold;
   }

}
