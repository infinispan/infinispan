package org.infinispan.context.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class ContextLock extends CompletableFuture<Void> {
   private static Log log = LogFactory.getLog(ContextLock.class);
   private static boolean trace = log.isTraceEnabled();

   private static final AtomicReferenceFieldUpdater requestorUpdater = AtomicReferenceFieldUpdater.newUpdater(ContextLock.class, Object.class, "requestor");

   private final Request owner;
   // This variable starts as Ownership and can become null or ContextLock
   private volatile Object requestor;

   private ContextLock(Request owner, Request requestor) {
      this.owner = owner;
      this.requestor = requestor;
   }

   public static Object init() {
      Request request = newRequest();
      request.criticalThread = Thread.currentThread();
      return request;
   }

   private static Request newRequest() {
      return new Request();
   }

   public static <LockOwner> CompletionStage<Void> enter(LockOwner lockHolder, AtomicReferenceFieldUpdater<LockOwner, Object> updater) {
      for (;;) {
         Object currentLock = updater.get(lockHolder);
         if (trace) {
            log.tracef("Current lock is %s", currentLock);
         }
         if (currentLock == null) {
            Request newRequest = newRequest();
            if (updater.compareAndSet(lockHolder, null, newRequest)) {
               newRequest.criticalThread = Thread.currentThread();
               return null;
            }
         } else if (currentLock instanceof Request) {
            ContextLock newLock = new ContextLock((Request) currentLock, newRequest());
            if (updater.compareAndSet(lockHolder, currentLock, newLock)) {
               return newLock;
            }
         } else if (currentLock instanceof ContextLock) {
            ContextLock myLock = ((ContextLock) currentLock).request(newRequest());
            if (myLock != null) {
               return myLock;
            }
         } else {
            throw new IllegalStateException("Unexpected context lock " + currentLock);
         }
      }
   }

   public static <LockOwner> void exit(LockOwner lockOwner, AtomicReferenceFieldUpdater<LockOwner, Object> updater) {
      Object currentLock = updater.get(lockOwner);
      for (;;) {
         if (currentLock == null) {
            throw new IllegalStateException("Double exit?");
         } else if (currentLock instanceof Request) {
            if (updater.compareAndSet(lockOwner, currentLock, null)) {
               return;
            }
         } else if (currentLock instanceof ContextLock) {
            ContextLock contextLock = (ContextLock) currentLock;
            Object requestor = contextLock.requestor();
            if (updater.compareAndSet(lockOwner, currentLock, requestor)) {
               if (requestor instanceof Request) {
                  ((Request) requestor).criticalThread = Thread.currentThread();
               } else {
                  ((ContextLock) requestor).owner.criticalThread = Thread.currentThread();
               }
               contextLock.complete(null);
               return;
            } else {
               throw new IllegalStateException("Someone else replaced the lock for " + updater.get(lockOwner));
            }
         } else {
            // e.g. other thread
            throw new IllegalStateException("Unexpected context lock " + currentLock);
         }
      }
   }

   public static boolean isOwned(Object contextLock) {
      if (contextLock instanceof Request) {
         return ((Request) contextLock).criticalThread == Thread.currentThread();
      } else if (contextLock instanceof ContextLock) {
         return ((ContextLock) contextLock).owner.criticalThread == Thread.currentThread();
      } else {
         return false;
      }
   }

   private Object requestor() {
      Object requestor = this.requestor;
      if (requestor instanceof Thread) {
         if (requestorUpdater.compareAndSet(this, requestor, null)) {
            return requestor;
         }
         requestor = this.requestor;
         assert requestor instanceof ContextLock : "Requestor is " + requestor;
      }
      return requestor;
   }

   /**
    * @param newRequest
    * @return ContextLock or null if we haven't been able to request position in chain
    */
   private ContextLock request(Request newRequest) {
      ContextLock self = this;
      for (;;) {
         Object requestor = self.requestor;
         if (requestor instanceof Request) {
            ContextLock newLock = new ContextLock((Request) requestor, newRequest);
            if (requestorUpdater.compareAndSet(self, requestor, newLock)) {
               return newLock;
            }
            requestor = self.requestor;
            if (requestor == null) {
               return null;
            } else {
               assert requestor instanceof ContextLock : "Requestor is " + requestor;
               self = (ContextLock) requestor;
            }
         } else {
            self = (ContextLock) requestor;
         }
      }
   }

   @Override
   public String toString() {
      return "C:" + owner.criticalThread + " -> " + requestor;
   }

   private static final class Request {
      // TODO: only for diagnostic purposes, remove me later and replace Request with singleton
      Thread criticalThread;

      @Override
      public String toString() {
         return "R:" + criticalThread;
      }
   }
}
