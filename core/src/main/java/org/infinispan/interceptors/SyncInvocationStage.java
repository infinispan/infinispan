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
   public Object addCallback(InvocationContext ctx, VisitableCommand command, InvocationCallback function) {
      try {
         return function.apply(ctx, command, rv, null);
      } catch (Throwable throwable) {
         return new SimpleAsyncInvocationStage(throwable);
      }
   }

   @Override
   public String toString() {
      return new StringBuilder("SyncInvocationStage(").append(rv).append(')').toString();
   }
}
