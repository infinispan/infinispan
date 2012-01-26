package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.statetransfer.StateTransferLock;

/**
 * Date: 1/26/12
 * Time: 11:18 AM
 *
 * @author pruivo
 */
public class TOVersionedReplicationInterceptor extends TOReplicationInterceptor {

    private StateTransferLock stateTransferLock;
    private CommandsFactory cf;

    @Inject
    public void init(StateTransferLock stateTransferLock, CommandsFactory cf) {
        this.stateTransferLock = stateTransferLock;
        this.cf = cf;
    }


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

            EntryVersionsMap versionsMap = new EntryVersionsMap();

            for (Object key : command.getAffectedKeys()) {
                versionsMap.put(key, (IncrementableEntryVersion) ctx.lookupEntry(key).getVersion());
            }

            ((VersionedPrepareCommand) command).setVersionsSeen(versionsMap);

            broadcastPrepare(ctx, command);
            ((LocalTxInvocationContext) ctx).remoteLocksAcquired(rpcManager.getTransport().getMembers());
        }
        return retVal;
    }
}
