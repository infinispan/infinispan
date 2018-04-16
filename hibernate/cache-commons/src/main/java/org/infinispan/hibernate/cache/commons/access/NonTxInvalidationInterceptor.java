/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.access;

import java.util.concurrent.CompletableFuture;

import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.hibernate.cache.commons.util.CacheCommandInitializer;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.interceptors.impl.InvalidationInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.transport.impl.VoidResponseCollector;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This interceptor should completely replace default InvalidationInterceptor.
 * We need to send custom invalidation commands with transaction identifier (as the invalidation)
 * since we have to do a two-phase invalidation (releasing the locks as JTA synchronization),
 * although the cache itself is non-transactional.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
@MBean(objectName = "Invalidation", description = "Component responsible for invalidating entries on remote caches when entries are written to locally.")
public class NonTxInvalidationInterceptor extends BaseInvalidationInterceptor {
	@Inject private CacheCommandInitializer commandInitializer;

	private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(InvalidationInterceptor.class);
   private static final Log ispnLog = LogFactory.getLog(NonTxInvalidationInterceptor.class);

   private final InvocationSuccessFunction handleWriteReturn = this::handleWriteReturn;
	private final InvocationSuccessFunction handleEvictReturn = this::handleEvictReturn;

	@Override
	public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
		assert command.hasAnyFlag(FlagBitSets.PUT_FOR_EXTERNAL_READ);
		return invokeNext(ctx, command);
	}

	@Override
	public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) {
		throw new UnsupportedOperationException("Unexpected replace");
	}

	@Override
	public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
		// This is how we differentiate write/remove and evict; remove sends BeginInvalidateComand while evict just InvalidateCommand
		boolean isEvict = !command.hasAnyFlag(FlagBitSets.FORCE_WRITE_LOCK);
      return invokeNextThenApply(ctx, command, isEvict ? handleEvictReturn : handleWriteReturn);
	}

	@Override
	public Object visitClearCommand(InvocationContext ctx, ClearCommand command) {
		Object retval = invokeNext(ctx, command);
		if (!isLocalModeForced(command)) {
			// just broadcast the clear command - this is simplest!
			if (ctx.isOriginLocal()) {
				command.setTopologyId(rpcManager.getTopologyId());
				if (isSynchronous(command)) {
					return asyncValue(rpcManager.invokeCommandOnAll(command, VoidResponseCollector.ignoreLeavers(), syncRpcOptions));
				} else {
					rpcManager.sendToAll(command, DeliverOrder.NONE);
				}
			}
		}
		return retval;
	}

	@Override
	public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) {
		throw new UnsupportedOperationException("Unexpected putAll");
	}

	private <T extends WriteCommand & RemoteLockCommand> CompletableFuture<?> invalidateAcrossCluster(
			T command, boolean isTransactional, Object key, Object keyLockOwner) {
		// increment invalidations counter if statistics maintained
		incrementInvalidations();
		InvalidateCommand invalidateCommand;
		if (!isLocalModeForced(command)) {
			if (isTransactional) {
				invalidateCommand = commandInitializer.buildBeginInvalidationCommand(
                  EnumUtil.EMPTY_BIT_SET, new Object[] { key }, keyLockOwner);
			}
			else {
            invalidateCommand = commandsFactory.buildInvalidateCommand(EnumUtil.EMPTY_BIT_SET, new Object[] {key });
			}
			invalidateCommand.setTopologyId(rpcManager.getTopologyId());
			if (log.isDebugEnabled()) {
				log.debug("Cache [" + rpcManager.getAddress() + "] replicating " + invalidateCommand);
			}

			if (isSynchronous(command)) {
				return rpcManager.invokeCommandOnAll(invalidateCommand, VoidResponseCollector.ignoreLeavers(), syncRpcOptions)
                  .toCompletableFuture();
			} else {
				rpcManager.sendToAll(invalidateCommand, DeliverOrder.NONE);
			}
		}
		return null;
	}

   @Override
   protected Log getLog() {
      return ispnLog;
   }

   private Object handleWriteReturn(InvocationContext ctx, VisitableCommand command, Object rv) {
		RemoveCommand removeCmd = (RemoveCommand) command;
		if ( removeCmd.isSuccessful()) {
			return invalidateAcrossCluster(removeCmd, true, removeCmd.getKey(), removeCmd.getKeyLockOwner());
		}
		return null;
	}

	private Object handleEvictReturn(InvocationContext ctx, VisitableCommand command, Object rv) {
		RemoveCommand removeCmd = (RemoveCommand) command;
		if ( removeCmd.isSuccessful()) {
			return invalidateAcrossCluster(removeCmd, false, removeCmd.getKey(), removeCmd.getKeyLockOwner());
		}
		return null;
	}

}
