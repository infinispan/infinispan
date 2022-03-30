package org.infinispan.commons.tx;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.transaction.Synchronization;
import javax.transaction.xa.XAResource;

/**
 * A default blocking implementation of {@link TransactionResourceConverter}.
 * <p>
 * It blocks the invocation thread, so it is not recommended to use.
 *
 * @since 14.0
 */
public enum DefaultResourceConverter implements TransactionResourceConverter {
   INSTANCE;

   @Override
   public AsyncSynchronization convertSynchronization(Synchronization synchronization) {
      return synchronization instanceof AsyncSynchronization ?
            (AsyncSynchronization) synchronization :
            new Sync(synchronization);
   }

   @Override
   public AsyncXaResource convertXaResource(XAResource resource) {
      return resource instanceof AsyncXaResource ?
            (AsyncXaResource) resource :
            new Xa(resource);
   }

   private static class Sync implements AsyncSynchronization {

      private final Synchronization synchronization;

      private Sync(Synchronization synchronization) {
         this.synchronization = synchronization;
      }

      @Override
      public CompletionStage<Void> asyncBeforeCompletion() {
         CompletableFuture<Void> cf = new CompletableFuture<>();
         try {
            synchronization.beforeCompletion();
            cf.complete(null);
         } catch (Throwable t) {
            cf.completeExceptionally(t);
         }
         return cf;
      }

      @Override
      public CompletionStage<Void> asyncAfterCompletion(int status) {
         CompletableFuture<Void> cf = new CompletableFuture<>();
         try {
            synchronization.afterCompletion(status);
            cf.complete(null);
         } catch (Throwable t) {
            cf.completeExceptionally(t);
         }
         return cf;
      }
   }

   private static class Xa implements AsyncXaResource {

      private final XAResource resource;

      private Xa(XAResource resource) {
         this.resource = resource;
      }

      @Override
      public CompletionStage<Void> asyncEnd(XidImpl xid, int flags) {
         CompletableFuture<Void> cf = new CompletableFuture<>();
         try {
            resource.end(xid, flags);
            cf.complete(null);
         } catch (Throwable t) {
            cf.completeExceptionally(t);
         }
         return cf;
      }

      @Override
      public CompletionStage<Integer> asyncPrepare(XidImpl xid) {
         CompletableFuture<Integer> cf = new CompletableFuture<>();
         try {
            cf.complete(resource.prepare(xid));
         } catch (Throwable t) {
            cf.completeExceptionally(t);
         }
         return cf;
      }

      @Override
      public CompletionStage<Void> asyncCommit(XidImpl xid, boolean onePhase) {
         CompletableFuture<Void> cf = new CompletableFuture<>();
         try {
            resource.commit(xid, onePhase);
            cf.complete(null);
         } catch (Throwable t) {
            cf.completeExceptionally(t);
         }
         return cf;
      }

      @Override
      public CompletionStage<Void> asyncRollback(XidImpl xid) {
         CompletableFuture<Void> cf = new CompletableFuture<>();
         try {
            resource.rollback(xid);
            cf.complete(null);
         } catch (Throwable t) {
            cf.completeExceptionally(t);
         }
         return cf;
      }
   }
}
