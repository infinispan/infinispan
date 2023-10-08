package org.infinispan.lock.impl.lock;

import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.OwnershipLevel;
import org.infinispan.lock.exception.ClusteredLockException;
import org.infinispan.lock.impl.entries.ClusteredLockKey;
import org.infinispan.lock.impl.entries.ClusteredLockState;
import org.infinispan.lock.impl.entries.ClusteredLockValue;
import org.infinispan.lock.impl.functions.IsLocked;
import org.infinispan.lock.impl.functions.LockFunction;
import org.infinispan.lock.impl.functions.UnlockFunction;
import org.infinispan.lock.impl.manager.EmbeddedClusteredLockManager;
import org.infinispan.lock.logging.Log;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.transport.Address;

/**
 * Implements {@link ClusteredLock} interface.
 * <p>
 * This lock implements a non reentrant where the ownership is {@link OwnershipLevel#NODE}.
 * <p>
 * <h2>Non reentrant lock, Owner Node</h2> <lu> <li>Originator of the requests is the {@link Address} of the {@link
 * org.infinispan.manager.EmbeddedCacheManager}</li> <li>When a lock is acquired by a Node, it cannot be re-acquired,
 * even by the actual node til the lock is released.</li> <li>The lock can be unlocked only by the lock owner, in this
 * case the node</li> <li>lock method does not expire til the lock is acquired, so this can cause thread starvation in
 * the actual implementation</li> </lu>
 * <p>
 * <h2>Partition handling</h2>
 *
 * @author Katia Aresti, karesti@redhat.com
 * @see <a href="https://infinispan.org/documentation/">Infinispan documentation</a>
 * @since 9.2
 */
public class ClusteredLockImpl implements ClusteredLock {
   private static final Log log = LogFactory.getLog(ClusteredLockImpl.class, Log.class);

   private final String name;
   private final ClusteredLockKey lockKey;
   private final AdvancedCache<ClusteredLockKey, ClusteredLockValue> clusteredLockCache;
   private final EmbeddedClusteredLockManager clusteredLockManager;
   private final FunctionalMap.ReadWriteMap<ClusteredLockKey, ClusteredLockValue> readWriteMap;
   private final Queue<RequestHolder> pendingRequests;
   private final Address originator;
   private final AtomicInteger viewChangeUnlockHappening = new AtomicInteger(0);
   private final RequestExpirationScheduler requestExpirationScheduler;
   private final ClusterChangeListener clusterChangeListener;
   private final LockReleasedListener lockReleasedListener;

   public ClusteredLockImpl(String name,
                            ClusteredLockKey lockKey,
                            AdvancedCache<ClusteredLockKey, ClusteredLockValue> clusteredLockCache,
                            EmbeddedClusteredLockManager clusteredLockManager) {
      this.name = name;
      this.lockKey = lockKey;
      this.clusteredLockCache = clusteredLockCache;
      this.clusteredLockManager = clusteredLockManager;
      this.pendingRequests = new ConcurrentLinkedQueue<>();
      this.readWriteMap = ReadWriteMapImpl.create(FunctionalMapImpl.create(clusteredLockCache));
      this.originator = clusteredLockCache.getCacheManager().getAddress();
      this.requestExpirationScheduler = new RequestExpirationScheduler(clusteredLockManager.getScheduledExecutorService());
      this.clusterChangeListener = new ClusterChangeListener();
      this.lockReleasedListener = new LockReleasedListener();
      this.clusteredLockCache.getCacheManager().addListener(clusterChangeListener);
      this.clusteredLockCache.addFilteredListener(lockReleasedListener, new ClusteredLockFilter(lockKey), null,
            Util.asSet(CacheEntryModified.class, CacheEntryRemoved.class));
   }

   public void stop() {
      clusteredLockCache.removeListener(clusterChangeListener);
      clusteredLockCache.removeListener(lockReleasedListener);
      requestExpirationScheduler.clear();
   }

   public abstract class RequestHolder<E> {
      protected final CompletableFuture<E> request;
      protected final String requestId;
      protected final Address requestor;

      public RequestHolder(Address requestor, CompletableFuture<E> request) {
         this.requestId = createRequestId();
         this.requestor = requestor;
         this.request = request;
      }

      public boolean isDone() {
         return request.isDone();
      }

      public void handleLockResult(Boolean result, Throwable ex) {
         if (ex != null) {
            log.errorf(ex, "LOCK[%s] Exception on lock request %s", getName(), this.toString());
            request.completeExceptionally(handleException(ex));
            return;
         }

         if (result == null) {
            if (log.isTraceEnabled()) {
               log.tracef("LOCK[%s] Result is null on request %s", getName(), this.toString());
            }
            request.completeExceptionally(new ClusteredLockException("Lock result is null, something is wrong"));
            return;
         }

         handle(result);
      }

      protected abstract void handle(Boolean result);

      protected abstract void forceFailed();

   }

   public class LockRequestHolder extends RequestHolder<Void> {

      public LockRequestHolder(Address requestor, CompletableFuture<Void> request) {
         super(requestor, request);
      }

      @Override
      protected void handle(Boolean result) {
         if (result) request.complete(null);
      }

      @Override
      protected void forceFailed() {
         request.complete(null);
      }

      @Override
      public String toString() {
         final StringBuilder sb = new StringBuilder("LockRequestHolder{");
         sb.append("name=").append(getName());
         sb.append(", requestId=").append(requestId);
         sb.append(", requestor=").append(requestor);
         sb.append(", completed=").append(request.isDone());
         sb.append(", completedExceptionally=").append(request.isCompletedExceptionally());
         sb.append('}');
         return sb.toString();
      }

   }

   public class TryLockRequestHolder extends RequestHolder<Boolean> {

      private final long time;
      private final TimeUnit unit;
      private boolean isScheduled;

      public TryLockRequestHolder(Address requestor, CompletableFuture<Boolean> request) {
         super(requestor, request);
         this.time = 0;
         this.unit = null;
      }

      public TryLockRequestHolder(Address requestor, CompletableFuture<Boolean> request, long time, TimeUnit unit) {
         super(requestor, request);
         this.time = time;
         this.unit = unit;
      }

      @Override
      protected void handle(Boolean result) {
         if (time <= 0) {
            // The answer has to be returned without holding the CompletableFuture
            if (log.isTraceEnabled()) {
               log.tracef("LOCK[%s] Result[%b] for request %s", getName(), result, this);
            }
            request.complete(result);
         } else if (result) {
            // The lock might have been acquired correctly
            if (log.isTraceEnabled()) {
               log.tracef("LOCK[%s] LockResult[%b] for %s", getName(), result, this);
            }
            request.complete(true);
            requestExpirationScheduler.abortScheduling(requestId);
            Boolean tryLockRealResult = request.join();
            if (!tryLockRealResult) {
               // Even if we complete true just before, the lock request can be completed false just before by the scheduler.
               // This means that tryLock reached the max time waiting before the lock was actually acquired
               // In this case, even if the lock was marked as acquired in the cache, it has to be released because the call expired.
               // We have to unlock the lock if the requestor and the requestId match.
               // Meanwhile another request for this owner might have locked it successfully and we don't want to unlock in that case
               unlock(requestId, Collections.singleton(requestor));
            }
         } else if (!isScheduled) {
            if (log.isTraceEnabled()) {
               log.tracef("LOCK[%s] Schedule for expiration %s", getName(), this);
            }
            // If the lock was not acquired, then schedule a complete false for the given timeout
            isScheduled = true;
            requestExpirationScheduler.scheduleForCompletion(requestId, request, time, unit);
         }
      }

      @Override
      protected void forceFailed() {
         request.complete(false);
      }

      @Override
      public String toString() {
         final StringBuilder sb = new StringBuilder("TryLockRequestHolder{");
         sb.append("name=").append(getName());
         sb.append(", requestId=").append(requestId);
         sb.append(", requestor=").append(requestor);
         sb.append(", time=").append(time);
         sb.append(", unit=").append(unit);
         sb.append(", completed=").append(request.isDone());
         sb.append(", completedExceptionally=").append(request.isCompletedExceptionally());
         sb.append('}');
         return sb.toString();
      }

      public boolean hasTimeout() {
         return time > 0;
      }
   }

   @Listener(clustered = true)
   class LockReleasedListener {

      @CacheEntryModified
      public void entryModified(CacheEntryModifiedEvent event) {
         ClusteredLockValue value = (ClusteredLockValue) event.getValue();
         if (value.getState() == ClusteredLockState.RELEASED) {
            if (log.isTraceEnabled()) {
               log.tracef("LOCK[%s] Lock has been released, %s notified", getName(), originator);
            }
            retryPendingRequests(value);
         }
      }

      @CacheEntryRemoved
      public void entryRemoved(CacheEntryRemovedEvent event) {
         while (!pendingRequests.isEmpty()) {
            RequestHolder requestHolder = pendingRequests.poll();
            requestHolder.handleLockResult(null, log.lockDeleted());
            requestExpirationScheduler.abortScheduling(requestHolder.requestId);
         }
      }
   }

   private void retryPendingRequests(ClusteredLockValue value) {
      if (isChangeViewUnlockInProgress()) {
         if (log.isTraceEnabled()) {
            log.tracef("LOCK[%s] Hold pending requests while view change unlock is happening in %s", getName(), originator);
         }
      } else {
         RequestHolder nextRequestor = null;
         if (log.isTraceEnabled()) {
            log.tracef("LOCK[%s] Pending requests size[%d] in %s", getName(), pendingRequests.size(), originator);
         }
         while (!pendingRequests.isEmpty() && (nextRequestor == null || nextRequestor.isDone() || isSameRequest(nextRequestor, value)))
            nextRequestor = pendingRequests.poll();

         if (nextRequestor != null) {
            if (log.isTraceEnabled()) {
               log.tracef("LOCK[%s] About to retry lock for %s", getName(), nextRequestor);
            }
            final RequestHolder requestor = nextRequestor;
            lock(requestor);
         }
      }
   }

   private void retryPendingRequests() {
      retryPendingRequests(null);
   }

   private boolean isSameRequest(RequestHolder nextRequestor, ClusteredLockValue value) {
      if (value == null) return false;

      return nextRequestor.requestId.equals(value.getRequestId()) && nextRequestor.requestor.equals(value.getOwner());
   }

   @Listener
   class ClusterChangeListener {

      @ViewChanged
      public void viewChange(ViewChangedEvent event) {
         if (log.isTraceEnabled()) {
            log.tracef("LOCK[%s] ViewChange event has been fired %s", getName(), originator);
         }

         List<Address> newMembers = event.getNewMembers();
         List<Address> oldMembers = event.getOldMembers();
         if (newMembers.size() <= 1 && oldMembers.size() > 2) {
            if (log.isTraceEnabled()) {
               log.tracef("LOCK[%s] A single new node %s is this notification. Do nothing", getName(), originator);
            }
            return;
         }

         Set<Address> leavingNodes = oldMembers.stream().filter(a -> !newMembers.contains(a)).collect(Collectors.toSet());

         if (leavingNodes.isEmpty()) {
            if (log.isTraceEnabled()) {
               log.tracef("LOCK[%s] Nothing to do, all nodes are present %s", getName(), originator);
            }
            return;
         }

         if (leavingNodes.size() >= newMembers.size() && oldMembers.size() > 2) {
            // If the oldMembers size is greater than 2, we do nothing because the other nodes will handle
            // If the cluster was formed by 2 members and one leaves, we should not enter here
            if (log.isTraceEnabled()) {
               log.tracef("LOCK[%s] Nothing to do, we are on a minority partition notification on %s", getName(), originator);
            }
            return;
         }

         if (clusteredLockManager.isDefined(name)) {
            if (log.isTraceEnabled()) {
               log.tracef("LOCK[%s] %s launches unlock for each leaving node", getName(), originator);
            }
            forceUnlockForLeavingMembers(leavingNodes);
         }
      }

      /**
       * This method forces unlock for each of the Address that is not present in the cluster. We don't know which node
       * holds the lock, so we force an unlock
       *
       * @param possibleOwners
       */
      private void forceUnlockForLeavingMembers(Set<Address> possibleOwners) {
         if (log.isTraceEnabled()) {
            log.tracef("LOCK[%s] Call force unlock for %s from %s ", getName(), possibleOwners, originator);
         }
         int viewChangeUnlockValue = viewChangeUnlockHappening.incrementAndGet();
         if (log.isTraceEnabled()) {
            log.tracef("LOCK[%s] viewChangeUnlockHappening value in %s ", getName(), viewChangeUnlockValue, originator);
         }
         unlock(null, possibleOwners)
               .whenComplete((unlockResult, ex) -> {
                  if (log.isTraceEnabled()) {
                     log.tracef("LOCK[%s] Force unlock call completed for %s from %s ", getName(), possibleOwners, originator);
                  }
                  int viewChangeUnlockValueAfterUnlock = viewChangeUnlockHappening.decrementAndGet();
                  if (log.isTraceEnabled()) {
                     log.tracef("LOCK[%s] viewChangeUnlockHappening value in %s ", getName(), viewChangeUnlockValueAfterUnlock, originator);
                  }
                  if (ex == null) {
                     if (log.isTraceEnabled()) {
                        log.tracef("LOCK[%s] Force unlock result %b for %s from %s ", getName(), unlockResult, possibleOwners, originator);
                     }
                  } else {
                     log.error(ex, log.unlockFailed(getName(), getOriginator()));
                     // TODO: handle the exception. Retry ? End all the pending requests in this lock ?
                  }

                  retryPendingRequests();
               });
      }
   }

   @Override
   public CompletableFuture<Void> lock() {
      if (log.isTraceEnabled()) {
         log.tracef("LOCK[%s] lock called from %s", getName(), originator);
      }
      CompletableFuture<Void> lockRequest = new CompletableFuture<>();
      lock(new LockRequestHolder(originator, lockRequest));
      return lockRequest;
   }

   private void lock(RequestHolder<Void> requestHolder) {
      if (requestHolder == null || requestHolder.isDone())
         return;

      pendingRequests.offer(requestHolder);
      if (isChangeViewUnlockInProgress()) {
         if (log.isTraceEnabled()) {
            log.tracef("LOCK[%s] View change unlock is happening in %s. Do not try to lock", getName(), originator);
         }
      } else {
         readWriteMap.eval(lockKey, new LockFunction(requestHolder.requestId, requestHolder.requestor)).whenComplete((lockResult, ex) -> {
            requestHolder.handleLockResult(lockResult, ex);
         });
      }
   }

   @Override
   public CompletableFuture<Boolean> tryLock() {
      if (log.isTraceEnabled()) {
         log.tracef("LOCK[%s] tryLock called from %s", getName(), originator);
      }
      CompletableFuture<Boolean> tryLockRequest = new CompletableFuture<>();
      tryLock(new TryLockRequestHolder(originator, tryLockRequest));
      return tryLockRequest;
   }

   @Override
   public CompletableFuture<Boolean> tryLock(long time, TimeUnit unit) {
      if (log.isTraceEnabled()) {
         log.tracef("LOCK[%s] tryLock with timeout (%d, %s) called from %s", getName(), time, unit, originator);
      }
      CompletableFuture<Boolean> tryLockRequest = new CompletableFuture<>();
      tryLock(new TryLockRequestHolder(originator, tryLockRequest, time, unit));
      return tryLockRequest;
   }

   private void tryLock(TryLockRequestHolder requestHolder) {
      if (requestHolder == null || requestHolder.isDone()) {
         return;
      }
      if (requestHolder.hasTimeout()) pendingRequests.offer(requestHolder);

      if (isChangeViewUnlockInProgress()) {
         requestHolder.handleLockResult(false, null);
      } else {
         readWriteMap.eval(lockKey, new LockFunction(requestHolder.requestId, requestHolder.requestor)).whenComplete((lockResult, ex) -> {
            requestHolder.handleLockResult(lockResult, ex);
         });
      }
   }

   @Override
   public CompletableFuture<Void> unlock() {
      if (log.isTraceEnabled()) {
         log.tracef("LOCK[%s] unlock called from %s", getName(), originator);
      }
      CompletableFuture<Void> unlockRequest = new CompletableFuture<>();

      readWriteMap.eval(lockKey, new UnlockFunction(originator)).whenComplete((unlockResult, ex) -> {
         if (ex == null) {
            if (log.isTraceEnabled()) {
               log.tracef("LOCK[%s] Unlock result for %s is %b", getName(), originator, unlockResult);
            }
            unlockRequest.complete(null);
         } else {
            unlockRequest.completeExceptionally(handleException(ex));
         }
      });
      return unlockRequest;
   }

   @Override
   public CompletableFuture<Boolean> isLocked() {
      if (log.isTraceEnabled()) {
         log.tracef("LOCK[%s] isLocked called from %s", getName(), originator);
      }
      CompletableFuture<Boolean> isLockedRequest = new CompletableFuture<>();
      readWriteMap.eval(lockKey, new IsLocked()).whenComplete((isLocked, ex) -> {
         if (ex == null) {
            isLockedRequest.complete(isLocked);
         } else {
            isLockedRequest.completeExceptionally(handleException(ex));
         }
      });
      return isLockedRequest;
   }

   @Override
   public CompletableFuture<Boolean> isLockedByMe() {
      if (log.isTraceEnabled()) {
         log.tracef("LOCK[%s] isLockedByMe called from %s", getName(), originator);
      }
      CompletableFuture<Boolean> isLockedByMeRequest = new CompletableFuture<>();
      readWriteMap.eval(lockKey, new IsLocked(originator)).whenComplete((isLockedByMe, ex) -> {
         if (ex == null) {
            isLockedByMeRequest.complete(isLockedByMe);
         } else {
            isLockedByMeRequest.completeExceptionally(handleException(ex));
         }
      });
      return isLockedByMeRequest;
   }

   private CompletableFuture<Boolean> unlock(String requestId, Set<Address> possibleOwners) {
      if (log.isTraceEnabled()) {
         log.tracef("LOCK[%s] unlock called for %s %s", getName(), requestId, possibleOwners);
      }
      CompletableFuture<Boolean> unlockRequest = new CompletableFuture<>();
      readWriteMap.eval(lockKey, new UnlockFunction(requestId, possibleOwners)).whenComplete((unlockResult, ex) -> {
         if (ex == null) {
            unlockRequest.complete(unlockResult);
         } else {
            unlockRequest.completeExceptionally(handleException(ex));
         }
      });
      return unlockRequest;
   }

   private String createRequestId() {
      return Util.threadLocalRandomUUID().toString();
   }

   private boolean isChangeViewUnlockInProgress() {
      return viewChangeUnlockHappening.get() > 0;
   }

   private Throwable handleException(Throwable ex) {
      Throwable lockException = ex;
      if (ex instanceof RemoteException) {
         lockException = ex.getCause();
      }
      if (!(lockException instanceof ClusteredLockException)) {
         lockException = new ClusteredLockException(ex);
      }
      return lockException;
   }

   public String getName() {
      return name;
   }

   public Object getOriginator() {
      return originator;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("ClusteredLockImpl{");
      sb.append("lock=").append(getName());
      sb.append(", originator=").append(originator);
      sb.append('}');
      return sb.toString();
   }
}
