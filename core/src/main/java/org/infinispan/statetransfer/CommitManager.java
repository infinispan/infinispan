package org.infinispan.statetransfer;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.Metadata;
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
   private final EquivalentConcurrentHashMapV8<Object, DiscardPolicy> tracker;
   private DataContainer dataContainer;
   private volatile boolean trackStateTransfer;
   private volatile boolean trackXSiteStateTransfer;

   public CommitManager(Equivalence<Object> keyEq) {
      tracker = new EquivalentConcurrentHashMapV8<Object, DiscardPolicy>(keyEq, AnyEquivalence.<DiscardPolicy>getInstance());
   }

   @Inject
   public final void inject(DataContainer dataContainer) {
      this.dataContainer = dataContainer;
   }

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
      }
   }

   /**
    * It tries to commit the cache entry. The entry is not committed if it is originated from state transfer and other
    * operation already has updated it.
    *
    * @param entry     the entry to commit
    * @param metadata  the entry's metadata
    * @param operation if {@code null}, it identifies this commit as originated from a normal operation. Otherwise, it
    *                  is originated from a state transfer (local or remote site)
    */
   public final void commit(final CacheEntry entry, final Metadata metadata, final Flag operation,
                            boolean l1Invalidation) {
      if (trace) {
         log.tracef("Trying to commit. Key=%s. Operation Flag=%s, L1 invalidation=%s", entry.getKey(), operation,
                    l1Invalidation);
      }
      if (l1Invalidation || (operation == null && !trackStateTransfer && !trackXSiteStateTransfer)) {
         //track == null means that it is a normal put and the tracking is not enabled!
         //if it is a L1 invalidation, commit without track it.
         if (trace) {
            log.tracef("Committing key=%s. It is a L1 invalidation or a normal put and no tracking is enabled!",
                       entry.getKey());
         }
         entry.commit(dataContainer, metadata);
         return;
      }
      if (isTrackDisabled(operation)) {
         //this a put for state transfer but we are not tracking it. This means that the state transfer has ended
         //or canceled due to a clear command.
         if (trace) {
            log.tracef("Not committing key=%s. It is a state transfer key but no track is enabled!", entry.getKey());
         }
         return;
      }
      tracker.compute(entry.getKey(), new EquivalentConcurrentHashMapV8.BiFun<Object, DiscardPolicy, DiscardPolicy>() {
         @Override
         public DiscardPolicy apply(Object o, DiscardPolicy discardPolicy) {
            if (discardPolicy != null && discardPolicy.ignore(operation)) {
               if (trace) {
                  log.tracef("Not committing key=%s. It was already overwritten! Discard policy=%s", entry.getKey(),
                             discardPolicy);
               }
               return discardPolicy;
            }
            entry.commit(dataContainer, metadata);
            DiscardPolicy newDiscardPolicy = calculateDiscardPolicy();
            if (trace) {
               log.tracef("Committed key=%s. Old discard policy=%s. New discard policy=%s", entry.getKey(),
                          discardPolicy, newDiscardPolicy);
            }
            return newDiscardPolicy;
         }
      });
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

   private void setTrack(Flag track, boolean value) {
      if (trace) {
         log.tracef("Set track to %s = %s", track, value);
      }
      switch (track) {
         case PUT_FOR_STATE_TRANSFER:
            this.trackStateTransfer = value;
            return;
         case PUT_FOR_X_SITE_STATE_TRANSFER:
            this.trackXSiteStateTransfer = value;
      }
   }

   private boolean isTrackDisabled(Flag track) {
      return (track == Flag.PUT_FOR_STATE_TRANSFER && !trackStateTransfer) ||
            (track == Flag.PUT_FOR_X_SITE_STATE_TRANSFER && !trackXSiteStateTransfer);
   }

   private DiscardPolicy calculateDiscardPolicy() {
      if (trackStateTransfer && trackXSiteStateTransfer) {
         return DiscardPolicy.DISCARD_ALL_STATE_TRANSFER;
      } else if (trackStateTransfer) {
         return DiscardPolicy.DISCARD_STATE_TRANSFER;
      } else if (trackXSiteStateTransfer) {
         return DiscardPolicy.DISCARD_X_SITE_STATE_TRANSFER;
      }
      return null;
   }

   private static enum DiscardPolicy {
      DISCARD_STATE_TRANSFER {
         @Override
         public boolean ignore(Flag operation) {
            return operation == Flag.PUT_FOR_STATE_TRANSFER;
         }
      },
      DISCARD_X_SITE_STATE_TRANSFER {
         @Override
         public boolean ignore(Flag operation) {
            return operation == Flag.PUT_FOR_X_SITE_STATE_TRANSFER;
         }
      },
      DISCARD_ALL_STATE_TRANSFER {
         @Override
         public boolean ignore(Flag operation) {
            return operation == Flag.PUT_FOR_STATE_TRANSFER ||
                  operation == Flag.PUT_FOR_X_SITE_STATE_TRANSFER;
         }
      };

      public abstract boolean ignore(Flag operation);
   }
}
