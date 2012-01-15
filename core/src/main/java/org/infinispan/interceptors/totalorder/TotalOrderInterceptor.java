package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * Date: 1/15/12
 * Time: 9:48 PM
 *
 * @author Pedro Ruivo
 */
public class TotalOrderInterceptor extends CommandInterceptor {
    protected Map<GlobalTransaction, LocalTransaction> localTransactionMap = new HashMap<GlobalTransaction, LocalTransaction>();

    @Override
    public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
        if(ctx.isOriginLocal()) {
            localTransactionMap.put(command.getGlobalTransaction(), (LocalTransaction) ctx.getCacheTransaction());
            return invokeNextInterceptor(ctx, command);
        } else {
            try {
                return invokeNextInterceptor(ctx, command);
            } finally {
                LocalTransaction localTransaction = localTransactionMap.remove(command.getGlobalTransaction());
                if(localTransaction != null) {

                }
            }
        }
    }

    @Override
    public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
        throw new UnsupportedOperationException("Lock interface not supported with total order protocol");
    }
}
