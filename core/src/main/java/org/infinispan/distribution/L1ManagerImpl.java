/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.config.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.AggregatingNotifyingFutureImpl;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class L1ManagerImpl implements L1Manager {
	
	private static final Log log = LogFactory.getLog(L1ManagerImpl.class);
	private final boolean trace = log.isTraceEnabled();
	
	private RpcManager rpcManager;
	private CommandsFactory commandsFactory;
	private int threshold;

	private final ConcurrentMap<Object, Collection<Address>> requestors;
	
	public L1ManagerImpl() {
	   requestors = new ConcurrentHashMap<Object, Collection<Address>>();
   }
	
   @Inject
   public void init(Configuration configuration, RpcManager rpcManager, CommandsFactory commandsFactory) {
   	this.rpcManager = rpcManager;
   	this.commandsFactory = commandsFactory;
   	this.threshold = configuration.getL1InvalidationThreshold();
   }
   
   public void addRequestor(Object key, Address origin) {
      //we do a plain get first as that's likely to be enough
      Collection<Address> as = requestors.get(key);
      
      if (as == null) {
         // only if needed we create a new HashSet, but make sure we don't replace another one being created
         as = new ConcurrentHashSet<Address>();
         as.add(origin);
         Collection<Address> previousAs = requestors.putIfAbsent(key, as);
         if (previousAs != null) {
            //another thread added it already, so use his copy and discard our proposed instance
            previousAs.add(origin);
         }
      } else {
         as.add(origin);
      }
   }
   
   public NotifyingNotifiableFuture<Object> flushCache(Collection<Object> keys, Object retval, Address origin) {
      if (trace) log.tracef("Invalidating L1 caches for keys %s", keys);
      
      NotifyingNotifiableFuture<Object> future = new AggregatingNotifyingFutureImpl(retval, 2);
      
      Collection<Address> invalidationAddresses = buildInvalidationAddressList(keys, origin);
      
      int nodes = invalidationAddresses.size();

      if (nodes > 0) {
         // No need to invalidate at all if there is no one to invalidate!
         boolean multicast = isUseMulticast(nodes);

         if (trace) log.tracef("There are %s nodes involved in invalidation. Threshold is: %s; using multicast: %s", nodes, threshold, multicast);
         
         if (multicast) {
         	if (trace) log.tracef("Invalidating keys %s via multicast", keys);
         	InvalidateCommand ic = commandsFactory.buildInvalidateFromL1Command(false, keys);
      		rpcManager.broadcastRpcCommandInFuture(ic, future);
         } else {
         	InvalidateCommand ic = commandsFactory.buildInvalidateFromL1Command(false, keys);
         	
            // Ask the caches who have requested from us to remove
            if (trace) log.tracef("Keys %s needs invalidation on %s", keys, invalidationAddresses);
            rpcManager.invokeRemotelyInFuture(invalidationAddresses, ic, future);
            return future;
         }    
      } else
         if (trace) log.trace("No L1 caches to invalidate");
      return future;
   }
   
   private Collection<Address> buildInvalidationAddressList(Collection<Object> keys, Address origin) {
   	Collection<Address> addresses = new HashSet<Address>(2);
   	
   	for (Object key : keys) {
   	   Collection<Address> as = requestors.remove(key);
   	   if (as != null) {
   		   addresses.addAll(as);
            if (origin != null && as.contains(origin)) addRequestor(key, origin);
         }
   	}
   	if (origin != null)
   		addresses.remove(origin);
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
