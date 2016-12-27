package org.infinispan.interceptors;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.util.Experimental;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.impl.BasicAsyncInvocationStage;
import org.infinispan.interceptors.impl.AsyncInvocationStage;
import org.infinispan.interceptors.impl.ExceptionStage;
import org.infinispan.interceptors.impl.ReturnValueStage;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Base class for an interceptor in the new asynchronous invocation chain.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@Experimental
public abstract class BaseAsyncInterceptor implements AsyncInterceptor {
   protected Configuration cacheConfiguration;
   private AsyncInterceptor nextInterceptor;
   private DDAsyncInterceptor nextDDInterceptor;

   @Inject
   public void inject(Configuration cacheConfiguration) {
      this.cacheConfiguration = cacheConfiguration;
   }

   /**
    * Used internally to set up the interceptor.
    */
   @Override
   public final void setNextInterceptor(AsyncInterceptor nextInterceptor) {
      this.nextInterceptor = nextInterceptor;
      this.nextDDInterceptor = nextInterceptor instanceof DDAsyncInterceptor ? (DDAsyncInterceptor) nextInterceptor : null;
   }

   /**
    * Invoke the next interceptor, possibly with a new command.
    *
    * <p>{@link InvocationStage} then allows the caller to add a callback that will be executed after the remaining
    * interceptors.</p>
    * <p>Note: {@code invokeNext(ctx, command)} does not throw exceptions. In order to handle exceptions from the
    * next interceptors, you <em>must</em> use
    * {@link InvocationStage#exceptionally(InvocationContext, VisitableCommand, InvocationExceptionHandler)},
    * {@link InvocationStage#handle(InvocationContext, VisitableCommand, InvocationFinallyHandler)}, or
    * {@link InvocationStage#compose(InvocationContext, VisitableCommand, InvocationComposeHandler)}</p>
    */
   public final InvocationStage invokeNext(InvocationContext ctx, VisitableCommand command) {
      try {
         if (nextDDInterceptor != null) {
            return (InvocationStage) command.acceptVisitor(ctx, nextDDInterceptor);
         } else {
            return nextInterceptor.visitCommand(ctx, command);
         }
      } catch (Throwable throwable) {
         return new ExceptionStage(throwable);
      }
   }

   /**
    * Return a value directly, skipping the remaining interceptors in the chain.
    */
   public static InvocationStage returnWith(Object returnValue) {
      return new ReturnValueStage(returnValue);
   }

   /**
    * Suspend the invocation until {@code delay} completes, then if successful invoke the next interceptor.
    * <p>
    * If {@code delay} completes exceptionally, skip the next interceptor and continue with the exception.
    */
   public final InvocationStage invokeNextAsync(InvocationContext ctx, VisitableCommand command, CompletableFuture<?> delay) {
      if (delay.isDone()) {
         InvocationStage stage;
         try {
            // Make sure the delay was successful
            delay.join();
            stage = invokeNext(ctx, command);
         } catch (Throwable t) {
            stage = new ExceptionStage(CompletableFutures.extractException(t));
         }
         return stage;
      }

      return new AsyncInvocationStage(ctx, command, delay)
            .thenCompose(ctx, command, (stage, rCtx, rCommand, rv) -> invokeNext(rCtx, rCommand));
   }

   /**
    * Suspend the invocation until {@code returnValueFuture} completes, then return its result and skip the remaining
    * interceptors.
    * <p>
    * The caller can continue invoking the next interceptor, e.g.
    * {@code goAsync2(ctx, command, v).thenApply(ctx, command, (rCtx, rCommand, rv, t) -> invokeNext(rCtx, rCommand))}
    */
   public static InvocationStage returnWithAsync(CompletableFuture<?> valueFuture) {
      if (valueFuture.isDone()) {
         InvocationStage stage;
         try {
            Object value = valueFuture.join();
            stage = new ReturnValueStage(value);
         } catch (Throwable t) {
            stage = new ExceptionStage(CompletableFutures.extractException(t));
         }
         return stage;
      }

      return new BasicAsyncInvocationStage(valueFuture);
   }
}
