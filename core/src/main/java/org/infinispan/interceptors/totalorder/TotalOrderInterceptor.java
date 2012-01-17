package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.totalorder.TotalOrderValidator;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Created by IntelliJ IDEA.
 * Date: 1/15/12
 * Time: 9:48 PM
 *
 * @author Pedro Ruivo
 */
public class TotalOrderInterceptor extends CommandInterceptor {

    private static final Log log = LogFactory.getLog(TotalOrderInterceptor.class);

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
            log.tracef("Received prepare command in total order with transaction %s",
                    Util.printPrettyGlobalTransaction(command.getGlobalTransaction()));
            totalOrderValidator.validateTransaction(command, ctx, getNext());
            return null;
        }
    }

    @Override
    public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
        throw new UnsupportedOperationException("Lock interface not supported with total order protocol");
    }
}
