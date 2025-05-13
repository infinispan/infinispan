package org.infinispan.hibernate.cache.commons.access;

import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.hibernate.cache.commons.util.BeginInvalidationCommand;
import org.infinispan.hibernate.cache.commons.util.EndInvalidationCommand;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.util.ByteString;

/**
 * Non-transactional counterpart of {@link TxPutFromLoadInterceptor}.
 * Invokes {@link PutFromLoadValidator#beginInvalidatingKey(Object, Object)} for each invalidation from
 * remote node ({@link BeginInvalidationCommand} and sends {@link EndInvalidationCommand} after the transaction
 * is complete, with help of {@link InvalidationSynchronization};
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class NonTxPutFromLoadInterceptor extends BaseCustomAsyncInterceptor {
	private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(NonTxPutFromLoadInterceptor.class);
	private final ByteString cacheName;
	private final PutFromLoadValidator putFromLoadValidator;

	@Inject RpcManager rpcManager;

	public NonTxPutFromLoadInterceptor(PutFromLoadValidator putFromLoadValidator, ByteString cacheName) {
		this.putFromLoadValidator = putFromLoadValidator;
		this.cacheName = cacheName;
	}

	@Override
	public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) {
		if (!ctx.isOriginLocal() && command instanceof BeginInvalidationCommand) {
			for (Object key : command.getKeys()) {
				putFromLoadValidator.beginInvalidatingKey(((BeginInvalidationCommand) command).getLockOwner(), key);
			}
		}
		return invokeNext(ctx, command);
	}

	public void endInvalidating(Object key, Object lockOwner, boolean successful) {
		assert lockOwner != null;
		if (!putFromLoadValidator.endInvalidatingKey(lockOwner, key, successful)) {
			log.failedEndInvalidating(key, cacheName);
		}
		rpcManager.sendToAll(new EndInvalidationCommand(cacheName, new Object[] {key}, lockOwner), DeliverOrder.NONE);
	}
}
