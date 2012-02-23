package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.VersionedEntryWrappingInterceptor;
import org.infinispan.util.Util;
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

   private boolean trace = false;

   @Start
   public void setLogLevel() {
      trace = log.isTraceEnabled();
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      String globalTransactionString = Util.prettyPrintGlobalTransaction(command.getGlobalTransaction());
      if (!ctx.isOriginLocal() || command.isReplayEntryWrapping()) {
         if(trace) {
            log.tracef("Received a prepare command. Transaction: %s, Versions: %s",
                  globalTransactionString, ((VersionedPrepareCommand) command).getVersionsSeen());
         }
         for (WriteCommand c : command.getModifications()) {
            c.acceptVisitor(ctx, entryWrappingVisitor);
         }
      }
      EntryVersionsMap newVersionData = null;

      Object retVal = invokeNextInterceptor(ctx, command);

      if (!ctx.isOriginLocal()) {
         newVersionData = cll.createNewVersionsAndCheckForWriteSkews(versionGenerator, ctx,
               (VersionedPrepareCommand) command);
         if (command.isOnePhaseCommit()) {
            if(trace) {
               log.tracef("Transaction %s is committing now. new versions are: %s",
                     globalTransactionString, newVersionData);
            }
            ctx.getCacheTransaction().setUpdatedEntryVersions(newVersionData);
            commitContextEntries(ctx);
         } else {
            if(trace) {
               log.tracef("Transaction %s will be committed in the 2nd phase. new versions are: %s",
                     globalTransactionString, newVersionData);
            }
         }
      }

      if (newVersionData != null) retVal = newVersionData;
      return retVal;
   }
}
