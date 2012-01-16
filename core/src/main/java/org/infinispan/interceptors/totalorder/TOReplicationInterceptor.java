package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.ReplicationInterceptor;
import org.infinispan.remoting.RpcException;
import org.infinispan.transaction.LocalTransaction;

/**
 * Date: 1/16/12
 * Time: 10:51 AM
 *
 * @author pruivo
 */
public class TOReplicationInterceptor extends ReplicationInterceptor {
    @Override
    protected void broadcastPrepare(TxInvocationContext context, PrepareCommand command) {
        boolean sync = configuration.getCacheMode() == Configuration.CacheMode.REPL_SYNC;
        rpcManager.broadcastRpcCommand(command, false);

        if(sync) {
            //this is only invoked in local context
            LocalTransaction localTransaction = (LocalTransaction) context.getCacheTransaction();
            try {
                localTransaction.awaitUntilModificationsApplied(configuration.getSyncReplTimeout());
            } catch (Throwable throwable) {
                throw new RpcException(throwable);
            }
        }
    }
}
