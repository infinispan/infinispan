package org.infinispan.remoting.inboundhandler.action;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.infinispan.distribution.Ownership;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.util.concurrent.locks.LockUtil;

/**
 * A base {@link Action} implementation for locking.
 * <p/>
 * This contains the basic steps for lock acquition: try to acquire, check when it is available and acquired (or not).
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public abstract class BaseLockingAction implements Action {

   private static final AtomicReferenceFieldUpdater<BaseLockingAction, InternalState> UPDATER =
         newUpdater(BaseLockingAction.class, InternalState.class, "internalState");

   private final ClusteringDependentLogic clusteringDependentLogic;
   private volatile InternalState internalState;

   public BaseLockingAction(ClusteringDependentLogic clusteringDependentLogic) {
      this.clusteringDependentLogic = clusteringDependentLogic;
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

   private void filterByPrimaryOwner(Collection<?> keys, Collection<Object> toAdd) {
      keys.forEach(key -> {
         if (LockUtil.getLockOwnership(key, clusteringDependentLogic) == Ownership.PRIMARY) {
            toAdd.add(key);
         }
      });
   }

   protected final List<Object> getAndUpdateFilteredKeys(ActionState state) {
      List<Object> filteredKeys = state.getFilteredKeys();
      if (filteredKeys == null) {
         Collection<?> rawKeys = state.getCommand().getKeysToLock();
         filteredKeys = new ArrayList<>(rawKeys.size());
         filterByPrimaryOwner(rawKeys, filteredKeys);
         state.updateFilteredKeys(filteredKeys);
      }
      return filteredKeys;
   }

   @Override
   public void cleanup(ActionState state) {
      //no-op by default
   }

   protected enum InternalState {
      INIT,
      CHECKING,
      CANCELED,
      MAKE_READY,
      READY
   }
}
