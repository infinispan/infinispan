package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.InvocationComposeHandler;
import org.infinispan.interceptors.InvocationComposeSuccessHandler;
import org.infinispan.interceptors.InvocationExceptionHandler;
import org.infinispan.interceptors.InvocationFinallyHandler;
import org.infinispan.interceptors.InvocationReturnValueHandler;
import org.infinispan.interceptors.InvocationSuccessHandler;

/**
 * Invocation stage representing a computation that already completed successfully.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class ReturnValueStage implements InvocationStage {
   private Object returnValue;

   public ReturnValueStage(Object returnValue) {
      this.returnValue = returnValue;
   }

   @Override
   public InvocationStage compose(InvocationContext ctx, VisitableCommand command, InvocationComposeHandler composeHandler) {
      try {
         return composeHandler.apply(this, ctx, command, returnValue, null);
      } catch (Throwable t) {
         return new ExceptionStage(t);
      }
   }

   @Override
   public InvocationStage thenCompose(InvocationContext ctx, VisitableCommand command, InvocationComposeSuccessHandler thenComposeHandler) {
      return compose(ctx, command, thenComposeHandler);
   }

   @Override
   public InvocationStage thenAccept(InvocationContext ctx, VisitableCommand command, InvocationSuccessHandler successHandler) {
      try {
         successHandler.accept(ctx, command, returnValue);
         return this;
      } catch (Throwable t) {
         return new ExceptionStage(t);
      }
   }

   @Override
   public InvocationStage thenApply(InvocationContext ctx, VisitableCommand command, InvocationReturnValueHandler returnValueHandler) {
      try {
         Object newReturnValue = returnValueHandler.apply(ctx, command, returnValue);
         updateValue(newReturnValue);
         return this;
      } catch (Throwable t) {
         return new ExceptionStage(t);
      }
   }

   @Override
   public InvocationStage exceptionally(InvocationContext ctx, VisitableCommand command, InvocationExceptionHandler exceptionHandler) {
      return this;
   }

   @Override
   public InvocationStage handle(InvocationContext ctx, VisitableCommand command, InvocationFinallyHandler finallyHandler) {
      try {
         finallyHandler.accept(ctx, command, returnValue, null);
         return this;
      } catch (Throwable t) {
         return new ExceptionStage(t);
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
