package org.infinispan.statetransfer;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ReadCommittedEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.persistence.manager.PersistenceManager;
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
@Scope(Scopes.NAMED_CACHE)
public class CommitManager {

   private static final Log log = LogFactory.getLog(CommitManager.class);

   // Package private for testing only.
   final Map<Integer, Map<Object, DiscardPolicy>> tracker = new ConcurrentHashMap<>();

   @Inject InternalDataContainer dataContainer;
   @Inject PersistenceManager persistenceManager;
   @Inject TimeService timeService;

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
         if (log.isTraceEnabled()) {
            log.tracef("Tracking is disabled. Clear tracker: %s", tracker);
         }
         tracker.clear();
      } else {
         tracker.values().removeIf(entries -> {
            entries.values().removeIf(policy -> policy.update(trackStateTransfer, trackXSiteStateTransfer));
            return entries.isEmpty();
         });
      }
   }

   /**
    * Stop tracking the entries for the given segment if state transfer tracking is enabled.
    *
    * @param flag: flag to verify if tracking is enabled.
    * @param segmentId: segment to stop tracking.
    */
   public final void stopTrackFor(Flag flag, int segmentId) {
      if (flag == Flag.PUT_FOR_STATE_TRANSFER && trackStateTransfer) {
         // We only remove entries that are not related to cross-site state transfer. Different sites may have
         // different configurations, thus a single entry may have different segment mapping varying from site to site.
         tracker.computeIfPresent(segmentId, (k, entries) -> {
            entries.values().removeIf(DiscardPolicy::stopForST);
            return entries.isEmpty() ? null : entries;
         });
      }
   }

   /**
    * It tries to commit the cache entry. The entry is not committed if it is originated from state transfer and other
    * operation already has updated it.
    * @param entry     the entry to commit
    * @param operation if {@code null}, it identifies this commit as originated from a normal operation. Otherwise, it
    * @param ctx
    */
   public final CompletionStage<Void> commit(final CacheEntry entry, final Flag operation, int segment,
                                             boolean l1Only, InvocationContext ctx) {
      if (log.isTraceEnabled()) {
         log.tracef("Trying to commit. Key=%s. Operation Flag=%s, L1 write/invalidation=%s", toStr(entry.getKey()),
               operation, l1Only);
      }
      if (l1Only || (operation == null && !trackStateTransfer && !trackXSiteStateTransfer)) {
         //track == null means that it is a normal put and the tracking is not enabled!
         //if it is a L1 invalidation, commit without track it.
         if (log.isTraceEnabled()) {
            log.tracef("Committing key=%s. It is a L1 invalidation or a normal put and no tracking is enabled!",
                  toStr(entry.getKey()));
         }
         return commitEntry(entry, segment, ctx);
      }
      if (isTrackDisabled(operation)) {
         //this a put for state transfer but we are not tracking it. This means that the state transfer has ended
         //or canceled due to a clear command.
         if (log.isTraceEnabled()) {
            log.tracef("Not committing key=%s. It is a state transfer key but no track is enabled!",
                  toStr(entry.getKey()));
         }
         return CompletableFutures.completedNull();
      }
      ByRef<CompletionStage<Void>> byRef = new ByRef<>(null);
      Function<DiscardPolicy, DiscardPolicy> renewPolicy = discardPolicy -> {
         if (discardPolicy != null && discardPolicy.ignore(operation)) {
            if (log.isTraceEnabled()) {
               log.tracef("Not committing key=%s. It was already overwritten! Discard policy=%s",
                     toStr(entry.getKey()), discardPolicy);
            }
            return discardPolicy;
         }
         byRef.set(commitEntry(entry, segment, ctx));
         DiscardPolicy newDiscardPolicy = calculateDiscardPolicy(operation);
         if (log.isTraceEnabled()) {
            log.tracef("Committed key=%s. Old discard policy=%s. New discard policy=%s", toStr(entry.getKey()),
                       discardPolicy, newDiscardPolicy);
         }
         return newDiscardPolicy;
      };
      tracker.compute(segment, (key, entries) -> {
         if (entries == null) {
            DiscardPolicy newDiscardPolicy = renewPolicy.apply(null);
            if (newDiscardPolicy != null) {
               entries = new ConcurrentHashMap<>();
               entries.put(entry.getKey(), newDiscardPolicy);
            }
         } else {
            entries.compute(entry.getKey(), (e, discardPolicy) -> renewPolicy.apply(discardPolicy));
         }

         return entries;
      });
      CompletionStage<Void> stage = byRef.get();
      if (stage != null) {
         return stage;
      }
      return CompletableFutures.completedNull();
   }

   private CompletionStage<Void> commitEntry(CacheEntry entry, int segment, InvocationContext ctx) {
      if (entry instanceof ReadCommittedEntry) {
         return ((ReadCommittedEntry) entry).commit(segment, dataContainer);
      } else {
         entry.commit(dataContainer);
      }
      return CompletableFutures.completedNull();
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
      if (log.isTraceEnabled()) {
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

      public final synchronized boolean ignore(Flag operation) {
         return (discardST && operation == Flag.PUT_FOR_STATE_TRANSFER) ||
               (discardXSiteST && operation == Flag.PUT_FOR_X_SITE_STATE_TRANSFER);
      }

      public synchronized boolean update(boolean discardST, boolean discardXSiteST) {
         this.discardST = discardST;
         this.discardXSiteST = discardXSiteST;
         return !this.discardST && !this.discardXSiteST;
      }

      public boolean stopForST() {
         return update(false, discardXSiteST);
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
