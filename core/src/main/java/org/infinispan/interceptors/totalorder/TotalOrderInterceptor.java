package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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

    private static final Log log = LogFactory.getLog(TotalOrderInterceptor.class);

    @Override
    public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
        if(ctx.isOriginLocal()) {
            localTransactionMap.put(command.getGlobalTransaction(), (LocalTransaction) ctx.getCacheTransaction());
            return invokeNextInterceptor(ctx, command);
        } else {
            log.tracef("Received prepare command in total order with transaction %s",
                    Util.printPrettyGlobalTransaction(command.getGlobalTransaction()));
            LocalTransaction localTransaction = localTransactionMap.remove(command.getGlobalTransaction());
            Object retVal = null;
            boolean exception = false;
            try {
                retVal = invokeNextInterceptor(ctx, command);
            } catch(Throwable t) {
                retVal = t;
                exception = true;
                throw t;
            } finally {
                if(localTransaction != null) {
                    localTransaction.addPrepareResult(retVal, exception);
                }
            }
            return retVal;
        }
    }

    @Override
    public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
        throw new UnsupportedOperationException("Lock interface not supported with total order protocol");
    }
}
