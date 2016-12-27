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
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Invocation stage that completed with an exception.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class ExceptionStage implements InvocationStage {
   Throwable throwable;

   public ExceptionStage(Throwable throwable) {
      this.throwable = throwable;
   }

   @Override
   public InvocationStage compose(InvocationContext ctx, VisitableCommand command, InvocationComposeHandler composeHandler) {
      try {
         return composeHandler.apply(this, ctx, command, null, throwable);
      } catch (Throwable t) {
         if (throwable != t) {
            t.addSuppressed(throwable);
            throwable = t;
         }
         return this;
      }
   }

   @Override
   public InvocationStage thenCompose(InvocationContext ctx, VisitableCommand command, InvocationComposeSuccessHandler thenComposeHandler) {
      return compose(ctx, command, thenComposeHandler);
   }

   @Override
   public InvocationStage thenAccept(InvocationContext ctx, VisitableCommand command, InvocationSuccessHandler successHandler) {
      // Skip the success handler
      return this;
   }

   @Override
   public InvocationStage thenApply(InvocationContext ctx, VisitableCommand command, InvocationReturnValueHandler returnValueHandler) {
      return this;
   }

   @Override
   public InvocationStage exceptionally(InvocationContext ctx, VisitableCommand command, InvocationExceptionHandler exceptionHandler) {
      try {
         Object rv = exceptionHandler.apply(ctx, command, throwable);
         return new ReturnValueStage(rv);
      } catch (Throwable t) {
         if (t != throwable) {
            t.addSuppressed(throwable);
            throwable = t;
         }
         return this;
      }
   }

   @Override
   public InvocationStage handle(InvocationContext ctx, VisitableCommand command, InvocationFinallyHandler finallyHandler) {
      try {
         finallyHandler.accept(ctx, command, null, throwable);
         return this;
      } catch (Throwable t) {
         if (t != throwable) {
            t.addSuppressed(throwable);
            throwable = t;
         }
         return this;
      }
   }

   @Override
   public CompletableFuture<Object> toCompletableFuture() {
      return CompletableFutures.completedExceptionFuture(throwable);
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
   public String toString() {
      return "ExceptionStage(" + Stages.className(throwable) + ")";
   }
}
