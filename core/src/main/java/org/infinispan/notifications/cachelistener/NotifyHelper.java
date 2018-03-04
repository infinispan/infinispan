package org.infinispan.notifications.cachelistener;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.FunctionalNotifier;
import org.infinispan.metadata.Metadata;

public class NotifyHelper {
   public static void entryCommitted(CacheNotifier notifier, FunctionalNotifier functionalNotifier,
                                     boolean created, boolean removed, boolean expired, CacheEntry entry,
                                     InvocationContext ctx, FlagAffectedCommand command, Object previousValue, Metadata previousMetadata) {
      // We only notify if there is no state transfer flag
      if (FlagBitSets.extractStateTransferFlag(ctx, command) != null) {
         return;
      }
      boolean isWriteOnly = (command instanceof WriteCommand) && ((WriteCommand) command).isWriteOnly();
      if (removed) {
         if (command instanceof RemoveCommand) {
            ((RemoveCommand) command).notify(ctx, previousValue, previousMetadata, false);
         } else if (command instanceof InvalidateCommand) {
            notifier.notifyCacheEntryInvalidated(entry.getKey(), entry.getValue(), entry.getMetadata(), false, ctx, command);
         } else {
            if (expired) {
               notifier.notifyCacheEntryExpired(entry.getKey(), previousValue, previousMetadata, ctx);
            } else {
               notifier.notifyCacheEntryRemoved(entry.getKey(), previousValue, previousMetadata, false, ctx, command);
            }

            // A write-only command only writes and so can't 100% guarantee
            // to be able to retrieve previous value when removed, so only
            // send remove event when the command is read-write.
            if (!isWriteOnly)
               functionalNotifier.notifyOnRemove(EntryViews.readOnly(entry.getKey(), previousValue, previousMetadata));

            functionalNotifier.notifyOnWriteRemove(entry.getKey());
         }
      } else {
         // Notify entry event after container has been updated
         if (created) {
            notifier.notifyCacheEntryCreated(
                  entry.getKey(), entry.getValue(), entry.getMetadata(), false, ctx, command);

            // A write-only command only writes and so can't 100% guarantee
            // that an entry has been created, so only send create event
            // when the command is read-write.
            if (!isWriteOnly)
               functionalNotifier.notifyOnCreate(entry);

            functionalNotifier.notifyOnWrite(entry);
         } else {
            notifier.notifyCacheEntryModified(entry.getKey(), entry.getValue(), entry.getMetadata(), previousValue,
                  previousMetadata, false, ctx, command);

            // A write-only command only writes and so can't 100% guarantee
            // that an entry has been created, so only send modify when the
            // command is read-write.
            if (!isWriteOnly)
               functionalNotifier.notifyOnModify(entry, previousValue, previousMetadata);

            functionalNotifier.notifyOnWrite(entry);
         }
      }
   }
}
