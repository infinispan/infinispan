package org.infinispan.xsite.irac;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;
import static org.infinispan.commons.util.Util.toStr;

import java.lang.invoke.MethodHandles;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Default implementation of {@link IracManagerKeyState}.
 *
 * @author Pedro Ruivo
 * @see IracManagerKeyState
 * @since 14
 */
class IracManagerKeyChangedState implements IracManagerKeyState {

   private static final AtomicReferenceFieldUpdater<IracManagerKeyChangedState, Status> STATUS_UPDATER = newUpdater(IracManagerKeyChangedState.class, Status.class, "status");
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final int segment;
   private final Object key;
   private final Object owner;
   private final boolean expiration;
   private volatile Status status = Status.READY;

   public IracManagerKeyChangedState(int segment, Object key, Object owner, boolean expiration) {
      this.segment = segment;
      this.key = Objects.requireNonNull(key);
      this.owner = Objects.requireNonNull(owner);
      this.expiration = expiration;
   }

   @Override
   public Object getKey() {
      return key;
   }

   @Override
   public Object getOwner() {
      return owner;
   }

   @Override
   public int getSegment() {
      return segment;
   }

   @Override
   public boolean isExpiration() {
      return expiration;
   }

   @Override
   public boolean isStateTransfer() {
      return false;
   }

   @Override
   public boolean canSend() {
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] State.setSending for key %s (status=%s)", toStr(key), status);
      }
      return STATUS_UPDATER.compareAndSet(this, Status.READY, Status.SENDING);
   }

   @Override
   public void retry() {
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] State.setRetry for key %s (status=%s)", toStr(key), status);
      }
      STATUS_UPDATER.compareAndSet(this, Status.SENDING, Status.READY);
   }

   @Override
   public boolean done() {
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] State.setCompleted for key %s (status=%s)", toStr(key), status);
      }
      return STATUS_UPDATER.compareAndSet(this, Status.SENDING, Status.DONE);
   }

   @Override
   public void discard() {
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] State.setDiscard for key %s (status=%s)", toStr(key), status);
      }
      STATUS_UPDATER.lazySet(this, Status.DONE);
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{" +
            "segment=" + segment +
            ", key=" + toStr(key) +
            ", owner=" + owner +
            ", expiration=" + expiration +
            ", isStateTransfer=" + isStateTransfer() +
            ", status=" + status +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof IracManagerKeyInfo)) return false;

      IracManagerKeyInfo that = (IracManagerKeyInfo) o;

      if (getSegment() != that.getSegment()) return false;
      if (!getKey().equals(that.getKey())) return false;
      return getOwner().equals(that.getOwner());
   }

   @Override
   public int hashCode() {
      int result = getSegment();
      result = 31 * result + getKey().hashCode();
      result = 31 * result + getOwner().hashCode();
      return result;
   }

   private enum Status {
      READY,
      SENDING,
      DONE
   }
}
