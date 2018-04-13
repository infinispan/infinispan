/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.access;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.VoidResponseCollector;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This interceptor acts as a replacement to the replication interceptor when the CacheImpl is configured with
 * ClusteredSyncMode as INVALIDATE.
 * <p/>
 * The idea is that rather than replicating changes to all caches in a cluster when write methods are called, simply
 * broadcast an {@link InvalidateCommand} on the remote caches containing all keys modified.  This allows the remote
 * cache to look up the value in a shared cache loader which would have been updated with the changes.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@MBean(objectName = "Invalidation", description = "Component responsible for invalidating entries on remote caches when entries are written to locally.")
public class TxInvalidationInterceptor extends BaseInvalidationInterceptor {
	private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog( TxInvalidationInterceptor.class );
   private static final Log ispnLog = LogFactory.getLog(TxInvalidationInterceptor.class);

   private final InvocationSuccessFunction broadcastClearIfNotLocal = this::broadcastClearIfNotLocal;
   private final InvocationSuccessFunction broadcastInvalidateForPrepare = this::broadcastInvalidateForPrepare;

   @Override
	public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
		if ( !isPutForExternalRead( command ) ) {
			return handleInvalidate( ctx, command, command.getKey() );
		}
		return invokeNext( ctx, command );
	}

	@Override
	public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) {
		return handleInvalidate( ctx, command, command.getKey() );
	}

	@Override
	public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
		return handleInvalidate( ctx, command, command.getKey() );
	}

	@Override
	public Object visitClearCommand(InvocationContext ctx, ClearCommand command) {
      return invokeNextThenApply(ctx, command, broadcastClearIfNotLocal);
	}

   private Object broadcastClearIfNotLocal(InvocationContext rCtx, VisitableCommand rCommand, Object rv) {
      FlagAffectedCommand flagCmd = (FlagAffectedCommand) rCommand;
      if ( !isLocalModeForced( flagCmd ) ) {
         // just broadcast the clear command - this is simplest!
         if ( rCtx.isOriginLocal() ) {
				((TopologyAffectedCommand) rCommand).setTopologyId(rpcManager.getTopologyId());
         	if (isSynchronous(flagCmd)) {
					// the result value will be ignored, we don't need to propagate rv
            	return asyncValue(rpcManager.invokeCommandOnAll(rCommand, VoidResponseCollector.ignoreLeavers(), syncRpcOptions));
				} else {
            	rpcManager.sendToAll(rCommand, DeliverOrder.NONE);
				}
         }
      }
      return rv;
   }

   @Override
	public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) {
		return handleInvalidate( ctx, command, command.getMap().keySet().toArray() );
	}

	@Override
	public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) {
      return invokeNextThenApply(ctx, command, broadcastInvalidateForPrepare);
	}

   private Object broadcastInvalidateForPrepare(InvocationContext rCtx, VisitableCommand rCommand, Object rv) throws Throwable {
      log.tracef( "Entering InvalidationInterceptor's prepare phase.  Ctx flags are empty" );
      // fetch the modifications before the transaction is committed (and thus removed from the txTable)
      TxInvocationContext txCtx = (TxInvocationContext) rCtx;
      if ( shouldInvokeRemoteTxCommand( txCtx ) ) {
         if ( txCtx.getTransaction() == null ) {
            throw new IllegalStateException( "We must have an associated transaction" );
         }

         PrepareCommand prepareCmd = (PrepareCommand) rCommand;
         List<WriteCommand> mods = Arrays.asList( prepareCmd.getModifications() );
			CompletionStage<Void> completion = broadcastInvalidateForPrepare(mods, txCtx);
			if (completion != null) {
				return asyncValue(completion);
			} else {
				return rv;
			}
		}
      else {
         log.tracef( "Nothing to invalidate - no modifications in the transaction." );
      }
      return rv;
   }

   @Override
	public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) {
		Object retVal = invokeNext( ctx, command );
		if ( ctx.isOriginLocal() ) {
			//unlock will happen async as it is a best effort
			boolean sync = !command.isUnlock();
			List<Address> members = getMembers();
			( (LocalTxInvocationContext) ctx ).remoteLocksAcquired(members);
			command.setTopologyId(rpcManager.getTopologyId());
			if (sync) {
				return asyncValue(rpcManager.invokeCommandOnAll(command, VoidResponseCollector.ignoreLeavers(), syncRpcOptions));
			} else {
				rpcManager.sendToAll(command, DeliverOrder.NONE);
			}
		}
		return retVal;
	}

	private Object handleInvalidate(InvocationContext ctx, WriteCommand command, Object... keys) {
      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         WriteCommand writeCmd = (WriteCommand) rCommand;
			if (!writeCmd.isSuccessful() || rCtx.isInTxScope()) {
				return rv;
			}
			if (keys == null || keys.length == 0) {
				return rv;
			}
			if (isLocalModeForced( writeCmd )) {
				return rv;
			}
			CompletionStage<Void> completion = invalidateAcrossCluster(isSynchronous(writeCmd), keys, rCtx);
			return completion != null ? asyncValue(completion) : rv;
		} );
	}

	private CompletionStage<Void> broadcastInvalidateForPrepare(List<WriteCommand> modifications, InvocationContext ctx) throws Throwable {
		// A prepare does not carry flags, so skip checking whether is local or not
		if ( ctx.isInTxScope() ) {
			if ( modifications.isEmpty() ) {
				return null;
			}

			InvalidationFilterVisitor filterVisitor = new InvalidationFilterVisitor( modifications.size() );
			filterVisitor.visitCollection( null, modifications );

			if ( filterVisitor.containsPutForExternalRead ) {
				log.debug( "Modification list contains a putForExternalRead operation.  Not invalidating." );
			}
			else if ( filterVisitor.containsLocalModeFlag ) {
				log.debug( "Modification list contains a local mode flagged operation.  Not invalidating." );
			}
			else {
				try {
					CompletionStage<Void> completion = invalidateAcrossCluster(defaultSynchronous, filterVisitor.result.toArray(), ctx);
					if (completion != null) {
						return completion.exceptionally(t -> {
							log.unableToRollbackInvalidationsDuringPrepare( t );
							throw CompletableFutures.asCompletionException(t);
						});
					}
				} catch (Throwable t) {
					log.unableToRollbackInvalidationsDuringPrepare( t );
					throw t;
				}
			}
		}
		return null;
	}

   @Override
   protected Log getLog() {
      return ispnLog;
   }

   public static class InvalidationFilterVisitor extends AbstractVisitor {

		Set<Object> result;
		public boolean containsPutForExternalRead = false;
		public boolean containsLocalModeFlag = false;

		public InvalidationFilterVisitor(int maxSetSize) {
			result = new HashSet<>( maxSetSize );
		}

		private void processCommand(FlagAffectedCommand command) {
			containsLocalModeFlag = containsLocalModeFlag || ( command.getFlags() != null && command.getFlags().contains( Flag.CACHE_MODE_LOCAL ) );
		}

		@Override
		public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
			processCommand( command );
			containsPutForExternalRead =
					containsPutForExternalRead || ( command.getFlags() != null && command.getFlags().contains( Flag.PUT_FOR_EXTERNAL_READ ) );
			result.add( command.getKey() );
			return null;
		}

		@Override
		public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
			processCommand( command );
			result.add( command.getKey() );
			return null;
		}

		@Override
		public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) {
			processCommand( command );
			result.addAll( command.getAffectedKeys() );
			return null;
		}
	}

	private CompletionStage<Void> invalidateAcrossCluster(boolean synchronous, Object[] keys, InvocationContext ctx) {
		// increment invalidations counter if statistics maintained
		incrementInvalidations();
		final InvalidateCommand invalidateCommand = commandsFactory.buildInvalidateCommand( EnumUtil.EMPTY_BIT_SET, keys );
		if ( log.isDebugEnabled() ) {
			log.debug( "Cache [" + rpcManager.getAddress() + "] replicating " + invalidateCommand );
		}

		TopologyAffectedCommand command = invalidateCommand;
		if ( ctx.isInTxScope() ) {
			TxInvocationContext txCtx = (TxInvocationContext) ctx;
			// A Prepare command containing the invalidation command in its 'modifications' list is sent to the remote nodes
			// so that the invalidation is executed in the same transaction and locks can be acquired and released properly.
			// This is 1PC on purpose, as an optimisation, even if the current TX is 2PC.
			// If the cache uses 2PC it's possible that the remotes will commit the invalidation and the originator rolls back,
			// but this does not impact consistency and the speed benefit is worth it.
			command = commandsFactory.buildPrepareCommand( txCtx.getGlobalTransaction(), Collections.<WriteCommand>singletonList( invalidateCommand ), true );
		}
		command.setTopologyId(rpcManager.getTopologyId());
		if (synchronous) {
			return rpcManager.invokeCommandOnAll(command, VoidResponseCollector.ignoreLeavers(), syncRpcOptions);
		} else {
			rpcManager.sendToAll(command, DeliverOrder.NONE);
		}
		return null;
	}
}
