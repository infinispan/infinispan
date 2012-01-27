package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.totalorder.TotalOrderValidator;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * Created to control the total order validation. It disable the possibility of acquiring locks during execution through
 * the cache API
 *
 * Created by IntelliJ IDEA.
 * Date: 1/15/12
 * Time: 9:48 PM
 *
 * @author Pedro Ruivo
 */
public class TotalOrderInterceptor extends CommandInterceptor {

    //private static final Log log = LogFactory.getLog(TotalOrderInterceptor.class);

    private TotalOrderValidator totalOrderValidator;

    @Inject
    public void inject(TotalOrderValidator totalOrderValidator) {
        this.totalOrderValidator = totalOrderValidator;
    }

    @Override
    public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
        if(ctx.isOriginLocal()) {
            totalOrderValidator.addLocalTransaction(command, ctx);
            return invokeNextInterceptor(ctx, command);
        } else {
            totalOrderValidator.validateTransaction(command, ctx, getNext());
            return null;
        }
    }

    @Override
    public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
        throw new UnsupportedOperationException("Lock interface not supported with total order protocol");
    }

    //The rollback and commit command are only invoked with repeatable read + write skew + versioning

    @Override
    public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
        GlobalTransaction gtx = command.getGlobalTransaction();
        try {
            if (!ctx.isOriginLocal()) {
                totalOrderValidator.markTransactionForRollback(gtx);
                totalOrderValidator.waitForTxPrepared(ctx, gtx);
            } else {
                //only send the rollback command is the transaction was prepared previously.
                //otherwise, doesn't send the rollback, because no locks are acquired remotely
                command.setShouldInvokedRemotely(totalOrderValidator.isTransactionPrepared(gtx));
            }
            return invokeNextInterceptor(ctx, command);
        } finally {
            totalOrderValidator.finishTransaction(gtx);
        }
    }

    @Override
    public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
        GlobalTransaction gtx = command.getGlobalTransaction();
        try {
            if (!ctx.isOriginLocal()) {
                totalOrderValidator.waitForTxPrepared(ctx, gtx);
            }
            return invokeNextInterceptor(ctx, command);
        } finally {
            totalOrderValidator.finishTransaction(gtx);
        }
    }
}
