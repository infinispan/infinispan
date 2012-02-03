package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.totalorder.TotalOrderValidator;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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

    private static final Log log = LogFactory.getLog(TotalOrderInterceptor.class);
    private boolean trace;

    private TotalOrderValidator totalOrderValidator;

    @Inject
    public void inject(TotalOrderValidator totalOrderValidator) {
        this.totalOrderValidator = totalOrderValidator;
    }

    @Start
    public void setLogLevel() {
        this.trace = log.isTraceEnabled();
    }

    @Override
    public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
        if (trace) {
            log.tracef("Visit Prepare Command. Transaction is %s, Affected keys are %s, Should invoke remotely? %s",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                    command.getAffectedKeys(),
                    ctx.hasModifications());
        }

        try {
            if(ctx.isOriginLocal()) {
                totalOrderValidator.addLocalTransaction(command.getGlobalTransaction(),
                        (LocalTransaction) ctx.getCacheTransaction());
                return invokeNextInterceptor(ctx, command);
            } else {
                totalOrderValidator.validateTransaction(command, ctx, getNext());
                return null;
            }
        } catch (Throwable t) {
            if (trace) {
                log.tracef("Exception caught while visiting prepare command. Transaction is %s, Local? %s, " +
                        "version seen are %s, error message is %s",
                        Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                        ctx.isOriginLocal(), ctx.getCacheTransaction().getUpdatedEntryVersions(),
                        t.getMessage());
            }
            throw t;
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

        if (trace) {
            log.tracef("Visit Rollback Command. Transaction is %s",
                    Util.prettyPrintGlobalTransaction(gtx));
        }

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
        } catch (Throwable t) {
            if (trace) {
                log.tracef("Exception caught while visiting rollback command. Transaction is %s, Local? %s, " +
                        "error message is %s",
                        Util.prettyPrintGlobalTransaction(gtx), ctx.isOriginLocal(), t.getMessage());
            }
            throw t;
        } finally {
            totalOrderValidator.finishTransaction(gtx);
        }
    }

    @Override
    public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
        GlobalTransaction gtx = command.getGlobalTransaction();

        if (trace) {
            log.tracef("Visit Commit Command. Transaction is %s",
                    Util.prettyPrintGlobalTransaction(gtx));
        }

        try {
            if (!ctx.isOriginLocal()) {
                totalOrderValidator.waitForTxPrepared(ctx, gtx);
            }
            return invokeNextInterceptor(ctx, command);
        } catch (Throwable t) {
            if (trace) {
                log.tracef("Exception caught while visiting commit command. Transaction is %s, Local? %s, " +
                        "version seen are %s, error message is %s",
                        Util.prettyPrintGlobalTransaction(gtx), ctx.isOriginLocal(),
                        ctx.getCacheTransaction().getUpdatedEntryVersions(), t.getMessage());
            }
            throw t;
        } finally {
            totalOrderValidator.finishTransaction(gtx);
        }
    }
}
