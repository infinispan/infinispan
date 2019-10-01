package org.infinispan.interceptors;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

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
   public <C extends VisitableCommand> Object thenApply(InvocationContext ctx, C command,
                           InvocationSuccessFunction<C> function) {
      try {
         return function.apply(ctx, command, rv);
      } catch (Throwable throwable) {
         return new ExceptionSyncInvocationStage(throwable);
      }
   }

   @Override
   public <C extends VisitableCommand> Object thenAccept(InvocationContext ctx, C command,
                            InvocationSuccessAction<C> action) {
      return thenAcceptMakeStage(ctx, command, action);
   }

   public <C extends VisitableCommand> Object andExceptionally(InvocationContext ctx, C command,
                                  InvocationExceptionFunction<C> function) {
      return this;
   }

   public <C extends VisitableCommand> Object andFinally(InvocationContext ctx, C command,
                            InvocationFinallyAction<C> action) {
      return andFinallyMakeStage(ctx, command, action);
   }

   public <C extends VisitableCommand> Object andHandle(InvocationContext ctx, C command,
                           InvocationFinallyFunction<C> function) {
      try {
         return function.apply(ctx, command, rv, null);
      } catch (Throwable throwable) {
         return new ExceptionSyncInvocationStage(throwable);
      }
   }

   @Override
   public <C extends VisitableCommand> Object addCallback(InvocationContext ctx, C command, InvocationCallback<C> function) {
      try {
         return function.apply(ctx, command, rv, null);
      } catch (Throwable throwable) {
         return new ExceptionSyncInvocationStage(throwable);
      }
   }

   /**
    * After the current stage completes successfully, invoke {@code function} and return its result.
    *
    * The result may be either a plain value, or a new {@link InvocationStage}.
    */
   public <C extends VisitableCommand> InvocationStage thenApplyMakeStage(InvocationContext ctx, C command,
                                             InvocationSuccessFunction<C> function) {
      try {
         return makeStage(function.apply(ctx, command, rv));
      } catch (Throwable throwable) {
         return new ExceptionSyncInvocationStage(throwable);
      }
   }

   public <C extends VisitableCommand> InvocationStage thenAcceptMakeStage(InvocationContext ctx, C command,
                                              InvocationSuccessAction<C> action) {
      try {
         action.accept(ctx, command, rv);
         return this;
      } catch (Throwable throwable) {
         return new ExceptionSyncInvocationStage(throwable);
      }
   }

   public <C extends VisitableCommand> InvocationStage andExceptionallyMakeStage(InvocationContext ctx, C command,
                                                    InvocationExceptionFunction<C> function) {
      return this;
   }

   public <C extends VisitableCommand> InvocationStage andFinallyMakeStage(InvocationContext ctx, C command,
                                              InvocationFinallyAction<C> action) {
      try {
         action.accept(ctx, command, rv, null);
         return this;
      } catch (Throwable throwable) {
         return new ExceptionSyncInvocationStage(throwable);
      }
   }

   public <C extends VisitableCommand> InvocationStage andHandleMakeStage(InvocationContext ctx, C command,
                                             InvocationFinallyFunction<C> function) {
      try {
         return makeStage(function.apply(ctx, command, rv, null));
      } catch (Throwable throwable) {
         return new ExceptionSyncInvocationStage(throwable);
      }
   }

   @Override
   public Object thenReturn(InvocationContext ctx, VisitableCommand command, Object returnValue) {
      return returnValue;
   }

   @Override
   public String toString() {
      return "SyncInvocationStage(" + rv + ')';
   }
}
