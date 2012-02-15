package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.transaction.WriteSkewHelper.readVersionsFromResponse;
import static org.infinispan.transaction.WriteSkewHelper.setVersionsSeenOnPrepareCommand;

/**
 * Date: 1/26/12
 * Time: 11:18 AM
 *
 * @author pruivo
 */
public class TOVersionedReplicationInterceptor extends TOReplicationInterceptor {
    private static final Log log = LogFactory.getLog(TOVersionedReplicationInterceptor.class);

    //Pedro -- copied from VersionedReplicationInterceptor
    @Override
    protected PrepareCommand buildPrepareCommandForResend(TxInvocationContext ctx, CommitCommand commit) {
        // Make sure this is 1-Phase!!
        PrepareCommand command = cf.buildVersionedPrepareCommand(commit.getGlobalTransaction(), ctx.getModifications(), true);

        super.buildPrepareCommandForResend(ctx, commit);
        // Build a map of keys to versions as they were seen by the transaction originator's transaction context
        EntryVersionsMap vs = new EntryVersionsMap();
        for (CacheEntry ce: ctx.getLookedUpEntries().values()) {
            vs.put(ce.getKey(), (IncrementableEntryVersion) ce.getVersion());
        }

        // Make sure this version map is attached to the prepare command so that lock owners can perform write skew checks
        ((VersionedPrepareCommand) command).setVersionsSeen(vs);
        return command;
    }

    @Override
    public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
        if (!(command instanceof VersionedPrepareCommand)) {
            throw new IllegalStateException("Total order versioned replication interceptor only knows how to " +
                    "process versioned prepare commands");
        }

        Object retVal = invokeNextInterceptor(ctx, command);
        if (shouldInvokeRemoteTxCommand(ctx)) {
            stateTransferLock.waitForStateTransferToEnd(ctx, command, -1);
            setVersionsSeenOnPrepareCommand((VersionedPrepareCommand) command, ctx);
            broadcastPrepare(ctx, command);
        }
        return retVal;
    }

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
        rpcManager.broadcastRpcCommand(command, false);

        if(trace) {
            log.tracef("Transaction [%s] sent. waiting until validation finishes",
                    globalTransactionString);
        }
        //this is only invoked in local context
        LocalTransaction localTransaction = (LocalTransaction) context.getCacheTransaction();
        try {
            Object retVal = localTransaction.awaitUntilModificationsApplied();

            if (retVal instanceof EntryVersionsMap) {
                readVersionsFromResponse(new SuccessfulResponse(retVal), context.getCacheTransaction());
            } else {
                throw new IllegalStateException("This must not happen! we must receive the versions");
            }
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
