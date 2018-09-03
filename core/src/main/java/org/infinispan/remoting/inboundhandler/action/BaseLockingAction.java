package org.infinispan.remoting.inboundhandler.action;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;

/**
 * A base {@link Action} implementation for locking.
 * <p/>
 * This contains the basic steps for lock acquisition: try to acquire, check when it is available and acquired (or not).
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public abstract class BaseLockingAction implements Action {

   private static final AtomicReferenceFieldUpdater<BaseLockingAction, InternalState> UPDATER =
         newUpdater(BaseLockingAction.class, InternalState.class, "internalState");

   private final DistributionManager distributionManager;
   private volatile InternalState internalState;

   public BaseLockingAction(DistributionManager distributionManager) {
      this.distributionManager = distributionManager;
      this.internalState = InternalState.INIT;
   }

   @Override
   public final ActionStatus check(ActionState state) {
      switch (internalState) {
         case INIT:
            return init(state);
         case CHECKING:
            return checking(state);
         case MAKE_READY:
            return ActionStatus.NOT_READY; //some thread is calculating the final state
         case READY:
            return ActionStatus.READY; //ready final state
         case CANCELED:
            return ActionStatus.CANCELED; //canceled final state
      }
      return ActionStatus.NOT_READY; //not ready
   }

   protected abstract ActionStatus checking(ActionState state);

   protected abstract ActionStatus init(ActionState state);

   protected final boolean cas(InternalState expectedState, InternalState newState) {
      return UPDATER.compareAndSet(this, expectedState, newState);
   }

   private void filterByLockOwner(Collection<?> keys, Collection<Object> toAdd) {
      LocalizedCacheTopology cacheTopology = distributionManager.getCacheTopology();
      for (Object key : keys) {
         if (cacheTopology.getDistribution(key).isPrimary()) {
            toAdd.add(key);
         }
      }
   }

   protected final List<Object> getAndUpdateFilteredKeys(ActionState state) {
      List<Object> filteredKeys = state.getFilteredKeys();
      if (filteredKeys == null) {
         RemoteLockCommand remoteLockCommand = state.getCommand();
         Collection<?> rawKeys = remoteLockCommand.getKeysToLock();
         filteredKeys = new ArrayList<>(rawKeys.size());
         filterByLockOwner(rawKeys, filteredKeys);
         state.updateFilteredKeys(filteredKeys);
      }
      return filteredKeys;
   }

   protected enum InternalState {
      INIT,
      CHECKING,
      CANCELED,
      MAKE_READY,
      READY
   }
}
