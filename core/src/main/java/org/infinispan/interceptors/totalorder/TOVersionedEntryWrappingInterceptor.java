package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.VersionedEntryWrappingInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Date: 1/26/12
 * Time: 11:00 AM
 *
 * @author pruivo
 */
public class TOVersionedEntryWrappingInterceptor extends VersionedEntryWrappingInterceptor {
    private static final Log log = LogFactory.getLog(TOVersionedEntryWrappingInterceptor.class);

    @Override
    public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
        if (!ctx.isOriginLocal() || command.isReplayEntryWrapping()) {
            for (WriteCommand c : command.getModifications()) {
                c.acceptVisitor(ctx, entryWrappingVisitor);
            }
        }
        EntryVersionsMap newVersionData= null;

        Object retVal = invokeNextInterceptor(ctx, command);

        if (!ctx.isOriginLocal()) {
            newVersionData = cll.createNewVersionsAndCheckForWriteSkews(versionGenerator, ctx,
                    (VersionedPrepareCommand) command);
            if (command.isOnePhaseCommit()) {
                log.tracef("Received a one phase prepare command. new versions are: %s" + newVersionData);
                ctx.getCacheTransaction().setUpdatedEntryVersions(newVersionData);
                commitContextEntries(ctx);
            }
        }

        if (newVersionData != null) retVal = newVersionData;
        return retVal;
    }
}
