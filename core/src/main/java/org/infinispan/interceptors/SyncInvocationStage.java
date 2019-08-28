package org.infinispan.interceptors;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.impl.SimpleAsyncInvocationStage;

/**
 * @author Dan Berindei
 * @since 9.0
 */
public class SyncInvocationStage extends InvocationStage {
   static SyncInvocationStage COMPLETED_NULL_STAGE = new SyncInvocationStage();

   private final Object rv;

   public SyncInvocationStage(Object rv) {
      this.rv = rv;
   }

   public SyncInvocationStage() {
      this.rv = null;
   }

   @Override
   public Object get() throws Throwable {
      return rv;
   }

   @Override
   public boolean isDone() {
      return true;
   }

   @Override
   public CompletableFuture<Object> toCompletableFuture() {
      return CompletableFuture.completedFuture(rv);
   }

   @Override
   public Object thenApply(InvocationContext ctx, VisitableCommand command,
                           InvocationSuccessFunction function) {
      try {
         return function.apply(ctx, command, rv);
      } catch (Throwable throwable) {
         return new SimpleAsyncInvocationStage(throwable);
      }
   }

   @Override
   public Object thenAccept(InvocationContext ctx, VisitableCommand command,
                            InvocationSuccessAction action) {
      return thenAcceptMakeStage(ctx, command, action);
   }

   public Object andExceptionally(InvocationContext ctx, VisitableCommand command,
                                  InvocationExceptionFunction function) {
      return this;
   }

   public Object andFinally(InvocationContext ctx, VisitableCommand command,
                            InvocationFinallyAction action) {
      return andFinallyMakeStage(ctx, command, action);
   }

   public Object andHandle(InvocationContext ctx, VisitableCommand command,
                           InvocationFinallyFunction function) {
      try {
         return function.apply(ctx, command, rv, null);
      } catch (Throwable throwable) {
         return new SimpleAsyncInvocationStage(throwable);
      }
   }

   @Override
   public Object addCallback(InvocationContext ctx, VisitableCommand command, InvocationCallback function) {
      try {
         return function.apply(ctx, command, rv, null);
      } catch (Throwable throwable) {
         return new SimpleAsyncInvocationStage(throwable);
      }
   }

   /**
    * After the current stage completes successfully, invoke {@code function} and return its result.
    *
    * The result may be either a plain value, or a new {@link InvocationStage}.
    */
   public InvocationStage thenApplyMakeStage(InvocationContext ctx, VisitableCommand command,
                                             InvocationSuccessFunction function) {
      try {
         return makeStage(function.apply(ctx, command, rv));
      } catch (Throwable throwable) {
         return new SimpleAsyncInvocationStage(throwable);
      }
   }

   public InvocationStage thenAcceptMakeStage(InvocationContext ctx, VisitableCommand command,
                                              InvocationSuccessAction action) {
      try {
         action.accept(ctx, command, rv);
         return this;
      } catch (Throwable throwable) {
         return new SimpleAsyncInvocationStage(throwable);
      }
   }

   public InvocationStage andExceptionallyMakeStage(InvocationContext ctx, VisitableCommand command,
                                                    InvocationExceptionFunction function) {
      return this;
   }

   public InvocationStage andFinallyMakeStage(InvocationContext ctx, VisitableCommand command,
                                              InvocationFinallyAction action) {
      try {
         action.accept(ctx, command, rv, null);
         return this;
      } catch (Throwable throwable) {
         return new SimpleAsyncInvocationStage(throwable);
      }
   }

   public InvocationStage andHandleMakeStage(InvocationContext ctx, VisitableCommand command,
                                             InvocationFinallyFunction function) {
      try {
         return makeStage(function.apply(ctx, command, rv, null));
      } catch (Throwable throwable) {
         return new SimpleAsyncInvocationStage(throwable);
      }
   }

   @Override
   public String toString() {
      return "SyncInvocationStage(" + rv + ')';
   }
}
