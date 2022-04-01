package org.infinispan.interceptors;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * A sync {@link InvocationStage} for {@link Throwable}.
 * <p>
 * It is similar to {@link SyncInvocationStage} but instead of being used with a successful value, it accepts a {@link
 * Throwable}
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
public class ExceptionSyncInvocationStage extends InvocationStage {

   private final Throwable throwable;

   public ExceptionSyncInvocationStage(Throwable throwable) {
      this.throwable = CompletableFutures.extractException(throwable);
   }

   @Override
   public Object thenApply(InvocationContext ctx, VisitableCommand command, InvocationSuccessFunction function) {
      return this;
   }

   @Override
   public Object thenAccept(InvocationContext ctx, VisitableCommand command, InvocationSuccessAction function) {
      return this;
   }

   @Override
   public Object andExceptionally(InvocationContext ctx, VisitableCommand command,
         InvocationExceptionFunction function) {
      try {
         return function.apply(ctx, command, throwable);
      } catch (Throwable t) {
         return new ExceptionSyncInvocationStage(t);
      }
   }

   @Override
   public Object andFinally(InvocationContext ctx, VisitableCommand command, InvocationFinallyAction action) {
      try {
         action.accept(ctx, command, null, throwable);
         return this;
      } catch (Throwable t) {
         return new ExceptionSyncInvocationStage(t);
      }
   }

   @Override
   public Object andHandle(InvocationContext ctx, VisitableCommand command, InvocationFinallyFunction function) {
      try {
         return function.apply(ctx, command, null, throwable);
      } catch (Throwable t) {
         return new ExceptionSyncInvocationStage(t);
      }
   }

   @Override
   public Object thenReturn(InvocationContext ctx, VisitableCommand command, Object returnValue) {
      return this;
   }

   @Override
   public Object get() throws Throwable {
      throw throwable;
   }

   @Override
   public boolean isDone() {
      return true;
   }

   @Override
   public CompletableFuture<Object> toCompletableFuture() {
      return CompletableFutures.completedExceptionFuture(throwable);
   }

   @Override
   public Object addCallback(InvocationContext ctx, VisitableCommand command, InvocationCallback function) {
      try {
         return function.apply(ctx, command, null, throwable);
      } catch (Throwable t) {
         return new ExceptionSyncInvocationStage(t);
      }
   }
}
