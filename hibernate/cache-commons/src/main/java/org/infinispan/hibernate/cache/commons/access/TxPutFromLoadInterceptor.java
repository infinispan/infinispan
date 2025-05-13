package org.infinispan.hibernate.cache.commons.access;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.hibernate.cache.commons.util.EndInvalidationCommand;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.interceptors.impl.BaseRpcInterceptor;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Intercepts transactions in Infinispan, calling {@link PutFromLoadValidator#beginInvalidatingKey(Object, Object)}
 * before locks are acquired (and the entry is invalidated) and sends {@link EndInvalidationCommand} to release
 * invalidation throught {@link PutFromLoadValidator#endInvalidatingKey(Object, Object)} after the transaction
 * is committed.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class TxPutFromLoadInterceptor extends BaseRpcInterceptor {
	private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(TxPutFromLoadInterceptor.class);
   private static final Log ispnLog = LogFactory.getLog(TxPutFromLoadInterceptor.class);
	private final PutFromLoadValidator putFromLoadValidator;
	private final ByteString cacheName;

	@Inject RpcManager rpcManager;
	@Inject InternalDataContainer dataContainer;
	@Inject DistributionManager distributionManager;

	public TxPutFromLoadInterceptor(PutFromLoadValidator putFromLoadValidator, ByteString cacheName) {
		this.putFromLoadValidator = putFromLoadValidator;
		this.cacheName = cacheName;
	}

	private void beginInvalidating(InvocationContext ctx, Object key) {
		TxInvocationContext txCtx = (TxInvocationContext) ctx;
		// make sure that the command is registered in the transaction
		txCtx.addAffectedKey(key);

		GlobalTransaction globalTransaction = txCtx.getGlobalTransaction();
		if (!putFromLoadValidator.beginInvalidatingKey(globalTransaction, key)) {
			throw log.failedInvalidatePendingPut(key, cacheName.toString());
		}
	}

	@Override
	public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
		if (!command.hasAnyFlag(FlagBitSets.PUT_FOR_EXTERNAL_READ)) {
			beginInvalidating(ctx, command.getKey());
		}
		return invokeNext(ctx, command);
	}

	@Override
	public Object visitRemoveExpiredCommand(InvocationContext ctx, RemoveExpiredCommand command) throws Throwable {
		return invokeNext(ctx, command);
	}

	@Override
	public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
		beginInvalidating(ctx, command.getKey());
		return invokeNext(ctx, command);
	}

	// We need to intercept PrepareCommand, not InvalidateCommand since the interception takes
	// place before EntryWrappingInterceptor and the PrepareCommand is multiplexed into InvalidateCommands
	// as part of EntryWrappingInterceptor
	@Override
	public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) {
		if (ctx.isOriginLocal()) {
			// We can't wait to commit phase to remove the entry locally (invalidations are processed in 1pc
			// on remote nodes, so only local case matters here). The problem is that while the entry is locked
			// reads still can take place and we can read outdated collection after reading updated entity
			// owning this collection from DB; when this happens, the version lock on entity cannot protect
			// us against concurrent modification of the collection. Therefore, we need to remove the entry
			// here (even without lock!) and let possible update happen in commit phase.
			for (WriteCommand wc : command.getModifications()) {
				for (Object key : wc.getAffectedKeys()) {
					dataContainer.remove(key);
				}
			}
		}
		else {
			for (WriteCommand wc : command.getModifications()) {
            Collection<?> keys = wc.getAffectedKeys();
				if (log.isTraceEnabled()) {
					log.tracef("Invalidating keys %s with lock owner %s", keys, ctx.getLockOwner());
				}
				for (Object key : keys ) {
					putFromLoadValidator.beginInvalidatingKey(ctx.getLockOwner(), key);
				}
			}
		}
		return invokeNext(ctx, command);
	}

	@Override
	public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) {
		if (log.isTraceEnabled()) {
			log.tracef( "Commit command received, end invalidation" );
		}

		return endInvalidationAndInvokeNextInterceptor(ctx, command);
	}

	@Override
	public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) {
		if (log.isTraceEnabled()) {
			log.tracef( "Rollback command received, end invalidation" );
		}

		return endInvalidationAndInvokeNextInterceptor(ctx, command);
	}

	protected Object endInvalidationAndInvokeNextInterceptor(TxInvocationContext<?> ctx, VisitableCommand command) {
		if (ctx.isOriginLocal()) {
			// We cannot use directly ctx.getAffectedKeys() and that includes keys from local-only operations.
			// During evictAll inside transaction this would cause unnecessary invalidate command
			if (!ctx.getModifications().isEmpty()) {
				Object[] keys = ctx.getModifications().stream()
					.flatMap(mod -> mod.getAffectedKeys().stream()).distinct().toArray();

				if (log.isTraceEnabled()) {
					log.tracef( "Sending end invalidation for keys %s asynchronously, modifications are %s",
						Arrays.toString(keys), ctx.getCacheTransaction().getModifications());
				}

				GlobalTransaction globalTransaction = ctx.getGlobalTransaction();
				EndInvalidationCommand commitCommand = new EndInvalidationCommand(cacheName, keys, globalTransaction);
				List<Address> members = distributionManager.getCacheTopology().getMembers();
				rpcManager.sendToMany(members, commitCommand, DeliverOrder.NONE);

				// If the transaction is not successful, *RegionAccessStrategy would not be called, therefore
				// we have to end invalidation from here manually (in successful case as well)
				for (Object key : keys) {
					putFromLoadValidator.endInvalidatingKey(globalTransaction, key);
				}
			}
		}
		return invokeNext(ctx, command);
	}

	@Override
   protected Log getLog() {
      return ispnLog;
   }
}
