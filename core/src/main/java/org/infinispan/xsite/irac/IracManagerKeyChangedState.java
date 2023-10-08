package org.infinispan.xsite.irac;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;
import static org.infinispan.commons.util.Util.toStr;

import java.lang.invoke.MethodHandles;
import java.util.BitSet;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

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

   final IracManagerKeyInfo keyInfo;
   private final boolean expiration;

   @GuardedBy("backupMissing")
   private final BitSet backupMissing;

   private volatile Status status = Status.READY;

   public IracManagerKeyChangedState(int segment, Object key, Object owner, boolean expiration, int numberOfBackups) {
      this.keyInfo = new IracManagerKeyInfo(segment, key, owner);
      this.expiration = expiration;
      BitSet backupMissing = new BitSet(numberOfBackups);
      backupMissing.set(0, numberOfBackups);
      this.backupMissing = backupMissing;
   }

   @Override
   public IracManagerKeyInfo getKeyInfo() {
      return keyInfo;
   }

   @Override
   public Object getKey() {
      return keyInfo.getKey();
   }

   @Override
   public Object getOwner() {
      return keyInfo.getOwner();
   }

   @Override
   public int getSegment() {
      return keyInfo.getSegment();
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
         log.tracef("[IRAC] State.setSending for key %s (status=%s)", toStr(getKey()), status);
      }
      return STATUS_UPDATER.compareAndSet(this, Status.READY, Status.SENDING);
   }

   @Override
   public void retry() {
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] State.setRetry for key %s (status=%s)", toStr(getKey()), status);
      }
      STATUS_UPDATER.compareAndSet(this, Status.SENDING, Status.READY);
   }

   @Override
   public boolean isDone() {
      return STATUS_UPDATER.get(this) == Status.DONE;
   }

   @Override
   public void discard() {
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] State.setDiscard for key %s (status=%s)", toStr(getKey()), status);
      }
      STATUS_UPDATER.lazySet(this, Status.DONE);
   }

   @Override
   public void successFor(IracXSiteBackup site) {
      synchronized (backupMissing) {
         backupMissing.clear(site.siteIndex());
         if (backupMissing.isEmpty()) {
            if (log.isTraceEnabled()) {
               log.tracef("[IRAC] State.setCompleted for key %s (status=%s)", toStr(getKey()), status);
            }
            STATUS_UPDATER.set(this, Status.DONE);
         }
      }
   }

   @Override
   public boolean wasSuccessful(IracXSiteBackup site) {
      synchronized (backupMissing) {
         return !backupMissing.get(site.siteIndex());
      }
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{" +
            "segment=" + keyInfo.getSegment() +
            ", key=" + toStr(keyInfo.getKey()) +
            ", owner=" + keyInfo.getOwner() +
            ", expiration=" + expiration +
            ", isStateTransfer=" + isStateTransfer() +
            ", status=" + status +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof IracManagerKeyChangedState that)) return false;

      return getSegment() == that.getSegment() &&
            Objects.equals(getKey(), that.getKey()) &&
            Objects.equals(getOwner(), that.getOwner());
   }

   @Override
   public int hashCode() {
      int result = getSegment();
      result = 31 * result + getKey().hashCode();
      result = 31 * result + getOwner().hashCode();
      return result;
   }

   // Package-private for testing purposes.
   enum Status {
      READY,
      SENDING,
      DONE
   }
}
