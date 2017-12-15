package org.infinispan.lock.impl.lock;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

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
import org.infinispan.lock.impl.log.Log;
import org.infinispan.lock.impl.manager.EmbeddedClusteredLockManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.partitionhandling.AvailabilityException;
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
 * @see <a href="http://infinispan.org/documentation/">Infinispan documentation</a>
 * @since 9.2
 */
public class ClusteredLockImpl implements ClusteredLock {
   private static final Log log = LogFactory.getLog(ClusteredLockImpl.class, Log.class);

   private final String name;
   private final ClusteredLockKey lockKey;
   private final EmbeddedClusteredLockManager clusteredLockManager;
   private final FunctionalMap.ReadWriteMap<ClusteredLockKey, ClusteredLockValue> readWriteMap;
   private final Queue<RequestHolder> pendingRequests;
   private final Object originator;

   public ClusteredLockImpl(String name,
                            ClusteredLockKey lockKey,
                            AdvancedCache<ClusteredLockKey, ClusteredLockValue> clusteredLockCache,
                            EmbeddedClusteredLockManager clusteredLockManager) {
      this.name = name;
      this.lockKey = lockKey;
      this.clusteredLockManager = clusteredLockManager;
      this.pendingRequests = new ConcurrentLinkedQueue<>();
      this.readWriteMap = ReadWriteMapImpl.create(FunctionalMapImpl.create(clusteredLockCache));
      originator = clusteredLockCache.getCacheManager().getAddress();
      clusteredLockCache.getCacheManager().addListener(new ClusterChangeListener());
      clusteredLockCache.addListener(new LockReleasedListener(), new ClusteredLockFilter(lockKey));
   }

   public abstract class RequestHolder<E> {
      protected final CompletableFuture<E> request;
      protected final String requestId;
      protected final Object requestor;

      public RequestHolder(Object requestor, CompletableFuture<E> request) {
         this.requestId = createRequestId();
         this.requestor = requestor;
         this.request = request;
      }

      public boolean isDone() {
         return request.isDone();
      }

      public void handleLockResult(Boolean result, Throwable ex) {
         if (ex != null) {
            log.trace("Exception on lock request " + this, ex);
            request.completeExceptionally(handleException(ex));
            return;
         }

         if (result == null) {
            log.trace("Result is null on request " + this);
            request.completeExceptionally(new ClusteredLockException("Lock result is null, something is wrong"));
            return;
         }

         handle(result);
      }

      protected abstract void handle(Boolean result);

      private String createRequestId() {
         return Util.threadLocalRandomUUID().toString();
      }
   }

   public class LockRequestHolder extends RequestHolder<Void> {

      public LockRequestHolder(Object requestor, CompletableFuture<Void> request) {
         super(requestor, request);
      }

      @Override
      protected void handle(Boolean result) {
         if (result) request.complete(null);
      }

      @Override
      public String toString() {
         final StringBuilder sb = new StringBuilder("LockRequestHolder{");
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

      public TryLockRequestHolder(Object requestor, CompletableFuture<Boolean> request) {
         super(requestor, request);
         this.time = 0;
         this.unit = null;
      }

      public TryLockRequestHolder(Object requestor, CompletableFuture<Boolean> request, long time, TimeUnit unit) {
         super(requestor, request);
         this.time = time;
         this.unit = unit;
      }

      @Override
      protected void handle(Boolean result) {
         if (time <= 0) {
            // The answer has to be returned without holding the CompletableFuture
            log.tracef("Return the request no for %s", this);
            request.complete(result);
         } else if (result) {
            // The lock might have been acquired correctly
            request.complete(true);
            Boolean tryLockRealResult = request.join();
            if (!tryLockRealResult) {
               // Even if we complete true just before, the lock request can be completed false just before by the scheduler.
               // This means that tryLock reached the max time waiting before the lock was actually acquired
               // In this case, even if the lock was marked as acquired in the cache, it has to be released because the call expired.
               // We have to unlock the lock if the requestor and the requestId match.
               // Meanwhile another request for this owner might have locked it successfully and we don't want to unlock in that case
               unlock(requestId, requestor);
            }
         } else if(!isScheduled){
            // If the lock was not acquired, then schedule a complete false for the given timeout
            isScheduled = true;
            clusteredLockManager.schedule(() -> request.complete(false), time, unit);
         }
      }

      @Override
      public String toString() {
         final StringBuilder sb = new StringBuilder("TryLockRequestHolder{");
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
            RequestHolder nextRequestor = null;
            while (!pendingRequests.isEmpty() && (nextRequestor == null || nextRequestor.isDone()))
               nextRequestor = pendingRequests.poll();

            final RequestHolder requestor = nextRequestor;
            clusteredLockManager.execute(() -> lock(requestor));
         }
      }

      @CacheEntryRemoved
      public void entryRemoved(CacheEntryRemovedEvent event) {
         while (!pendingRequests.isEmpty()) {
            pendingRequests.poll().handleLockResult(null, log.lockDeleted());
         }
      }
   }

   @Listener
   class ClusterChangeListener {

      @ViewChanged
      public void viewChange(ViewChangedEvent event) {
         log.trace("viewChange event has been fired");
         List<Address> newMembers = event.getNewMembers();
         List<Address> oldMembers = event.getOldMembers();

         // TODO: Split brain and leaving nodes, handle this better
         if(newMembers.size() > 1 || (oldMembers.size() == 2 && newMembers.size() == 1)) {
            // There has to be at least 2 members
            // FIXME: Reentrant locks, can the node rejoin and reacquire the lock before the release actually happens ?? Use the requestId
            oldMembers.stream().filter(a -> !newMembers.contains(a)).forEach(notPresent -> {
               try {
                  if (clusteredLockManager.isDefined(name)) {
                     clusteredLockManager.execute(() -> unlock(null, notPresent)
                           .exceptionally(ex -> {
                              log.error("Unlock failed wrong", ex);
                              return null;
                           }));
                  }
               } catch (AvailabilityException ex) {
                  log.error("Unable to release due to cluster change", ex);
               }
            });
         }
      }
   }

   @Override
   public CompletableFuture<Void> lock() {
      log.tracef("lock called from %s", originator);
      CompletableFuture<Void> lockRequest = new CompletableFuture<>();
      lock(new LockRequestHolder(originator, lockRequest));
      return lockRequest;
   }

   private void lock(RequestHolder<Void> requestHolder) {
      if (requestHolder == null || requestHolder.isDone())
         return;

      pendingRequests.offer(requestHolder);
      readWriteMap.eval(lockKey, new LockFunction(requestHolder.requestId, requestHolder.requestor))
            .whenComplete((lockResult, ex) -> requestHolder.handleLockResult(lockResult, ex));
   }

   @Override
   public CompletableFuture<Boolean> tryLock() {
      log.tracef("tryLock called from %s", originator);
      CompletableFuture<Boolean> tryLockRequest = new CompletableFuture<>();
      tryLock(new TryLockRequestHolder(originator, tryLockRequest));
      return tryLockRequest;
   }

   @Override
   public CompletableFuture<Boolean> tryLock(long time, TimeUnit unit) {
      log.tracef("tryLock with timeout (%d, %s) called from %s", time, unit, originator);
      CompletableFuture<Boolean> tryLockRequest = new CompletableFuture<>();
      tryLock(new TryLockRequestHolder(originator, tryLockRequest, time, unit));
      return tryLockRequest;
   }

   private void tryLock(TryLockRequestHolder requestHolder) {
      if (requestHolder == null || requestHolder.isDone()) {
         return;
      }
      if (requestHolder.hasTimeout()) pendingRequests.offer(requestHolder);

      readWriteMap.eval(lockKey, new LockFunction(requestHolder.requestId, requestHolder.requestor)).whenComplete((lockResult, ex) -> {
         requestHolder.handleLockResult(lockResult, ex);
      });
   }

   @Override
   public CompletableFuture<Void> unlock() {
      log.tracef("unlock called from %s", originator);
      CompletableFuture<Void> unlockRequest = new CompletableFuture<>();
      readWriteMap.eval(lockKey, new UnlockFunction(originator)).whenComplete((lockResult, ex) -> {
         if (ex == null) {
            unlockRequest.complete(null);
         } else {
            unlockRequest.completeExceptionally(handleException(ex));
         }
      });
      return unlockRequest;
   }

   @Override
   public CompletableFuture<Boolean> isLocked() {
      log.tracef("isLocked called from %s", originator);
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
      log.tracef("isLockedByMe called from %s", originator);
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

   private CompletableFuture<Void> unlock(String requestId, Object owner) {
      log.tracef("unlock called for requestId %s for possible owner %s", owner);
      CompletableFuture<Void> unlockRequest = new CompletableFuture<>();
      readWriteMap.eval(lockKey, new UnlockFunction(requestId, owner)).whenComplete((lockResult, ex) -> {
         if (ex == null) {
            unlockRequest.complete(null);
         } else {
            unlockRequest.completeExceptionally(handleException(ex));
         }
      });
      return unlockRequest;
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
}
