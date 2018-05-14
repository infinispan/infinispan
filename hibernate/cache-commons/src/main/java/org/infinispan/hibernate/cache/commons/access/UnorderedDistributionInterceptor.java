/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.access;

import java.util.List;

import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.distribution.NonTxDistributionInterceptor;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Since the data handled in {@link TombstoneCallInterceptor} or {@link VersionedCallInterceptor}
 * does not rely on the order how these are applied (the updates are commutative), this interceptor
 * simply sends any command to all other owners without ordering them through primary owner.
 * Note that {@link LockingInterceptor} is required in the stack as locking on backup is not guaranteed
 * by primary owner.
 */
public class UnorderedDistributionInterceptor extends NonTxDistributionInterceptor {
	private static Log log = LogFactory.getLog(UnorderedDistributionInterceptor.class);
	private static final boolean trace = log.isTraceEnabled();

	@Inject private DistributionManager distributionManager;
	private boolean isReplicated;

	@Start
	public void start() {
		isReplicated = cacheConfiguration.clustering().cacheMode().isReplicated();
	}

	@Override
	public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
		if (command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
			// for state-transfer related writes
			return invokeNext(ctx, command);
		}
		int commandTopologyId = command.getTopologyId();
      LocalizedCacheTopology cacheTopology = distributionManager.getCacheTopology();
		int currentTopologyId = cacheTopology.getTopologyId();
		if (commandTopologyId != -1 && currentTopologyId != commandTopologyId) {
			throw new OutdatedTopologyException("Cache topology changed while the command was executing: expected " +
				commandTopologyId + ", got " + currentTopologyId);
		}

		if (isReplicated) {
			// local result is always ignored
			return invokeNextAndHandle(ctx, command, (rCtx, rCommand, rv, throwable) ->
					invokeRemotelyAsync(null, rCtx, (WriteCommand) rCommand));
		}
		else {
			List<Address> owners = cacheTopology.getDistribution(command.getKey()).writeOwners();
			if (owners.contains(rpcManager.getAddress())) {
				return invokeNextAndHandle( ctx, command, (rCtx, rCommand, rv, throwable) ->
                  invokeRemotelyAsync(owners, rCtx, (WriteCommand) rCommand));
			}
			else {
				log.tracef("Not invoking %s on %s since it is not an owner", command, rpcManager.getAddress());
            if (ctx.isOriginLocal() && command.isSuccessful()) {
               // This is called with the entry locked. In order to avoid deadlocks we must not wait for RPC while
               // holding the lock, therefore we'll return a future and wait for it in LockingInterceptor after
               // unlocking (and committing) the entry.
					if (isSynchronous(command)) {
						return rpcManager.invokeCommand(owners, command, MapResponseCollector.ignoreLeavers(owners.size()),
																  rpcManager.getSyncRpcOptions());
					} else {
						rpcManager.sendToMany(owners, command, DeliverOrder.NONE);
					}
				}
            return null;
			}
		}

	}

   public Object invokeRemotelyAsync(List<Address> finalOwners, InvocationContext rCtx, WriteCommand writeCmd) {
      if (rCtx.isOriginLocal() && writeCmd.isSuccessful()) {
         // This is called with the entry locked. In order to avoid deadlocks we must not wait for RPC while
         // holding the lock, therefore we'll return a future and wait for it in LockingInterceptor after
         // unlocking (and committing) the entry.
			if (isSynchronous(writeCmd)) {
				if (finalOwners != null) {
					return rpcManager.invokeCommand(
                  finalOwners, writeCmd, MapResponseCollector.ignoreLeavers(finalOwners.size()), rpcManager.getSyncRpcOptions());
				} else {
					return rpcManager.invokeCommandOnAll(writeCmd, MapResponseCollector.ignoreLeavers(), rpcManager.getSyncRpcOptions());
				}
			} else {
				rpcManager.sendToMany(finalOwners, writeCmd, DeliverOrder.NONE);
			}
      }
      return null;
   }
}
