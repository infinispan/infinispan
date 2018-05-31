package org.infinispan.statetransfer;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.container.impl.MergeOnStore;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ReadCommittedEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Keeps track of the keys updated by normal operation and state transfer. Since the command processing happens
 * concurrently with the state transfer, it needs to keep track of the keys updated by normal command in order to reject
 * the updates from the state transfer. It assumes that the keys from normal operations are most recent thant the ones
 * received by state transfer.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class CommitManager {

   private static final Log log = LogFactory.getLog(CommitManager.class);
   private static final boolean trace = log.isTraceEnabled();
   private final ConcurrentMap<Object, DiscardPolicy> tracker = new ConcurrentHashMap<>();

   @Inject private DataContainer dataContainer;
   @Inject private PersistenceManager persistenceManager;
   @Inject private TimeService timeService;

   private volatile boolean trackStateTransfer;
   private volatile boolean trackXSiteStateTransfer;

   /**
    * It starts tracking keys committed. All the keys committed will be flagged with this flag. State transfer received
    * after the key is tracked will be discarded.
    *
    * @param track Flag to start tracking keys for local site state transfer or for remote site state transfer.
    */
   public final void startTrack(Flag track) {
      setTrack(track, true);
   }

   /**
    * It stops tracking keys committed.
    *
    * @param track Flag to stop tracking keys for local site state transfer or for remote site state transfer.
    */
   public final void stopTrack(Flag track) {
      setTrack(track, false);
      if (!trackStateTransfer && !trackXSiteStateTransfer) {
         if (trace) {
            log.tracef("Tracking is disabled. Clear tracker: %s", tracker);
         }
         tracker.clear();
      } else {
         for (Iterator<Map.Entry<Object, DiscardPolicy>> iterator = tracker.entrySet().iterator();
              iterator.hasNext(); ) {
            if (iterator.next().getValue().update(trackStateTransfer, trackXSiteStateTransfer)) {
               iterator.remove();
            }
         }
      }
   }

   /**
    * It tries to commit the cache entry. The entry is not committed if it is originated from state transfer and other
    * operation already has updated it.
    * @param entry     the entry to commit
    * @param operation if {@code null}, it identifies this commit as originated from a normal operation. Otherwise, it
    * @param ctx
    */
   public final void commit(final CacheEntry entry, final Flag operation, int segment,
                            boolean l1Only, InvocationContext ctx) {
      if (trace) {
         log.tracef("Trying to commit. Key=%s. Operation Flag=%s, L1 write/invalidation=%s", toStr(entry.getKey()),
               operation, l1Only);
      }
      if (l1Only || (operation == null && !trackStateTransfer && !trackXSiteStateTransfer)) {
         //track == null means that it is a normal put and the tracking is not enabled!
         //if it is a L1 invalidation, commit without track it.
         if (trace) {
            log.tracef("Committing key=%s. It is a L1 invalidation or a normal put and no tracking is enabled!",
                  toStr(entry.getKey()));
         }
         commitEntry(entry, segment, ctx);
         return;
      }
      if (isTrackDisabled(operation)) {
         //this a put for state transfer but we are not tracking it. This means that the state transfer has ended
         //or canceled due to a clear command.
         if (trace) {
            log.tracef("Not committing key=%s. It is a state transfer key but no track is enabled!",
                  toStr(entry.getKey()));
         }
         return;
      }
      tracker.compute(entry.getKey(), (o, discardPolicy) -> {
         if (discardPolicy != null && discardPolicy.ignore(operation)) {
            if (trace) {
               log.tracef("Not committing key=%s. It was already overwritten! Discard policy=%s",
                     toStr(entry.getKey()), discardPolicy);
            }
            return discardPolicy;
         }
         commitEntry(entry, segment, ctx);
         DiscardPolicy newDiscardPolicy = calculateDiscardPolicy(operation);
         if (trace) {
            log.tracef("Committed key=%s. Old discard policy=%s. New discard policy=%s", toStr(entry.getKey()),
                       discardPolicy, newDiscardPolicy);
         }
         return newDiscardPolicy;
      });
   }

   private void commitEntry(CacheEntry entry, int segment, InvocationContext ctx) {
      if (!entry.isEvicted() && !entry.isRemoved() && entry.getValue() instanceof MergeOnStore) {
         PersistenceUtil.loadAndComputeInDataContainer(dataContainer, segment, persistenceManager, entry.getKey(), ctx,
               timeService, (k, oldEntry, factory) -> {
            Object newValue = ((MergeOnStore) entry.getValue()).merge(oldEntry == null ? null : oldEntry.getValue());
            if (newValue == null) {
               return null;
            }
            return factory.create(k, newValue, entry.getMetadata());
         });
      } else {
         if (segment != SegmentSpecificCommand.UNKNOWN_SEGMENT && entry instanceof ReadCommittedEntry) {
            ((ReadCommittedEntry) entry).commit(segment, dataContainer);
         } else {
            entry.commit(dataContainer);
         }
      }
   }

   /**
    * @return {@code true} if the flag is being tracked, {@code false} otherwise.
    */
   public final boolean isTracking(Flag trackFlag) {
      switch (trackFlag) {
         case PUT_FOR_STATE_TRANSFER:
            return trackStateTransfer;
         case PUT_FOR_X_SITE_STATE_TRANSFER:
            return trackXSiteStateTransfer;
      }
      return false;
   }

   /**
    * @return {@code true} if no keys are tracked, {@code false} otherwise.
    */
   public final boolean isEmpty() {
      return tracker.isEmpty();
   }

   @Override
   public String toString() {
      return "CommitManager{" +
            "tracker=" + tracker.size() + " key(s)" +
            ", trackStateTransfer=" + trackStateTransfer +
            ", trackXSiteStateTransfer=" + trackXSiteStateTransfer +
            '}';
   }

   private void setTrack(Flag track, boolean value) {
      if (trace) {
         log.tracef("Set track to %s = %s", track, value);
      }
      switch (track) {
         case PUT_FOR_STATE_TRANSFER:
            this.trackStateTransfer = value;
            break;
         case PUT_FOR_X_SITE_STATE_TRANSFER:
            this.trackXSiteStateTransfer = value;
            break;
      }
   }

   private boolean isTrackDisabled(Flag track) {
      return (track == Flag.PUT_FOR_STATE_TRANSFER && !trackStateTransfer) ||
            (track == Flag.PUT_FOR_X_SITE_STATE_TRANSFER && !trackXSiteStateTransfer);
   }

   private DiscardPolicy calculateDiscardPolicy(Flag operation) {
      boolean discardStateTransfer = trackStateTransfer && operation != Flag.PUT_FOR_STATE_TRANSFER;
      boolean discardXSiteStateTransfer = trackXSiteStateTransfer && operation != Flag.PUT_FOR_X_SITE_STATE_TRANSFER;
      if (!discardStateTransfer && !discardXSiteStateTransfer) {
         return null;
      }
      return new DiscardPolicy(discardStateTransfer, discardXSiteStateTransfer);
   }

   private static class DiscardPolicy {
      private boolean discardST;
      private boolean discardXSiteST;

      private DiscardPolicy(boolean discardST, boolean discardXSiteST) {
         this.discardST = discardST;
         this.discardXSiteST = discardXSiteST;
      }

      public synchronized final boolean ignore(Flag operation) {
         return (discardST && operation == Flag.PUT_FOR_STATE_TRANSFER) ||
               (discardXSiteST && operation == Flag.PUT_FOR_X_SITE_STATE_TRANSFER);
      }

      public synchronized boolean update(boolean discardST, boolean discardXSiteST) {
         this.discardST = discardST;
         this.discardXSiteST = discardXSiteST;
         return !this.discardST && !this.discardXSiteST;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         DiscardPolicy that = (DiscardPolicy) o;

         return discardST == that.discardST && discardXSiteST == that.discardXSiteST;

      }

      @Override
      public int hashCode() {
         int result = (discardST ? 1 : 0);
         result = 31 * result + (discardXSiteST ? 1 : 0);
         return result;
      }

      @Override
      public String toString() {
         return "DiscardPolicy{" +
               "discardStateTransfer=" + discardST +
               ", discardXSiteStateTransfer=" + discardXSiteST +
               '}';
      }
   }
}
