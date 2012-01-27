package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.ReplicationInterceptor;
import org.infinispan.remoting.RpcException;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * knows how to broadcast the prepare command in total order
 * Date: 1/16/12
 * Time: 10:51 AM
 *
 * @author pruivo
 */
public class TOReplicationInterceptor extends ReplicationInterceptor {

    private static final Log log = LogFactory.getLog(TOReplicationInterceptor.class);

    @Override
    protected void broadcastPrepare(TxInvocationContext context, PrepareCommand command) {
        boolean trace = log.isTraceEnabled();
        String globalTransactionString = Util.prettyPrintGlobalTransaction(command.getGlobalTransaction());

        if(!command.isTotalOrdered()) {
            super.broadcastPrepare(context, command);
            return;
        }

        if(trace) {
            log.tracef("Broadcasting transaction %s with Total Order", globalTransactionString);
        }

        //broadcast the command
        boolean sync = configuration.getCacheMode() == Configuration.CacheMode.REPL_SYNC;
        rpcManager.broadcastRpcCommand(command, false);

        if(sync) {
            //in sync mode, blocks in the LocalTransaction
            if(trace) {
                log.tracef("Transaction [%s] sent in synchronous mode. waiting until modification is applied",
                        globalTransactionString);
            }
            //this is only invoked in local context
            LocalTransaction localTransaction = (LocalTransaction) context.getCacheTransaction();
            try {
                localTransaction.awaitUntilModificationsApplied(configuration.getSyncReplTimeout());
            } catch (Throwable throwable) {
                throw new RpcException(throwable);
            } finally {
                if(trace) {
                    log.tracef("Transaction [%s] finishes the waiting time",
                            globalTransactionString);
                }
            }
        }
    }
}
