package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.InvocationComposeHandler;
import org.infinispan.interceptors.InvocationComposeSuccessHandler;
import org.infinispan.interceptors.InvocationExceptionHandler;
import org.infinispan.interceptors.InvocationFinallyHandler;
import org.infinispan.interceptors.InvocationReturnValueHandler;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.InvocationSuccessHandler;

/**
 * Invocation stage representing a computation that already completed successfully.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class ReturnValueStage extends AbstractInvocationStage {
   private Object returnValue;

   public ReturnValueStage(InvocationContext ctx, VisitableCommand command, Object returnValue) {
      super(ctx, command);
      this.returnValue = returnValue;
   }

   @Override
   public InvocationStage compose(InvocationComposeHandler composeHandler) {
      try {
         AbstractInvocationStage stage = (AbstractInvocationStage) composeHandler.apply(this, ctx, command,
               returnValue, null);
         return stage.toInvocationStage(ctx, command);
      } catch (Throwable t) {
         return new ExceptionStage(ctx, command, t);
      }
   }

   @Override
   public InvocationStage thenCompose(InvocationComposeSuccessHandler thenComposeHandler) {
      return compose(thenComposeHandler);
   }

   @Override
   public InvocationStage thenAccept(InvocationSuccessHandler successHandler) {
      try {
         successHandler.accept(ctx, command, returnValue);
         return this;
      } catch (Throwable t) {
         return new ExceptionStage(ctx, command, t);
      }
   }

   @Override
   public InvocationStage thenApply(InvocationReturnValueHandler returnValueHandler) {
      try {
         Object newReturnValue = returnValueHandler.apply(ctx, command, returnValue);
         updateValue(newReturnValue);
         return this;
      } catch (Throwable t) {
         return new ExceptionStage(ctx, command, t);
      }
   }

   @Override
   public InvocationStage exceptionally(InvocationExceptionHandler exceptionHandler) {
      return this;
   }

   @Override
   public InvocationStage handle(InvocationFinallyHandler finallyHandler) {
      try {
         finallyHandler.accept(ctx, command, returnValue, null);
         return this;
      } catch (Throwable t) {
         return new ExceptionStage(ctx, command, t);
      }
   }

   @Override
   public CompletableFuture<Object> toCompletableFuture() {
      return CompletableFuture.completedFuture(returnValue);
   }

   @Override
   public Object get() throws Throwable {
      return returnValue;
   }

   @Override
   public boolean isDone() {
      return true;
   }

   private void updateValue(Object newReturnValue) {
      if (returnValue != newReturnValue) {
         returnValue = newReturnValue;
      }
   }

   @Override
   public String toString() {
      return "ReturnValueStage(" + Stages.className(returnValue) + ")";
   }
}
