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
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Invocation stage that completed with an exception.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class ExceptionStage extends AbstractInvocationStage {
   Throwable throwable;

   public ExceptionStage(InvocationContext ctx, VisitableCommand command, Throwable throwable) {
      super(ctx, command);
      this.throwable = throwable;
   }

   @Override
   public InvocationStage compose(InvocationComposeHandler composeHandler) {
      try {
         AbstractInvocationStage stage = (AbstractInvocationStage) composeHandler.apply(this, ctx,
               command, null, throwable);
         return stage.toInvocationStage(ctx, command);
      } catch (Throwable t) {
         if (throwable != t) {
            t.addSuppressed(throwable);
            throwable = t;
         }
         return this;
      }
   }

   @Override
   public InvocationStage thenCompose(InvocationComposeSuccessHandler thenComposeHandler) {
      return compose(thenComposeHandler);
   }

   @Override
   public InvocationStage thenAccept(InvocationSuccessHandler successHandler) {
      // Skip the success handler
      return this;
   }

   @Override
   public InvocationStage thenApply(InvocationReturnValueHandler returnValueHandler) {
      return this;
   }

   @Override
   public InvocationStage exceptionally(InvocationExceptionHandler exceptionHandler) {
      try {
         Object rv = exceptionHandler.apply(ctx, command, throwable);
         return new ReturnValueStage(ctx, command, rv);
      } catch (Throwable t) {
         if (t != throwable) {
            t.addSuppressed(throwable);
            throwable = t;
         }
         return this;
      }
   }

   @Override
   public InvocationStage handle(InvocationFinallyHandler finallyHandler) {
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
