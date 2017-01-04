package org.infinispan.interceptors;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.util.Experimental;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.impl.InvocationStageImpl;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.function.TetraConsumer;
import org.infinispan.util.function.TriFunction;

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
    * Return a value directly, skipping the remaining interceptors in the chain.
    */
   public static InvocationStage completedStage(Object returnValue) {
      return InvocationStageImpl.makeSuccessful(returnValue);
   }

   /**
    * Return a value or rethrow an exception directly, skipping the remaining interceptors in the chain.
    *
    * Especially useful for callbacks that need to perform some work and then continue with the same value or exception.
    */
   public static InvocationStage completedStage(Object returnValue, Throwable throwable) {
      return InvocationStageImpl.makeSynchronous(returnValue, throwable);
   }

   public static <T> T rethrowAsCompletedException(Throwable throwable) {
      throw CompletableFutures.asCompletionException(throwable);
   }

   /**
    * Suspend the invocation until {@code returnValueFuture} completes, then return its result and skip the remaining
    * interceptors.
    * <p>
    * The caller can continue invoking the next interceptor, e.g.
    * {@code goAsync2(ctx, command, v).thenApply(ctx, command, (rCtx, rCommand, rv, t) -> invokeNext(rCtx, rCommand))}
    */
   public static InvocationStage asyncStage(CompletableFuture<?> valueFuture) {
      return InvocationStageImpl.makeAsynchronous(valueFuture);
   }

   /**
    * Invoke the next interceptor, possibly with a new command.
    *
    * <p>{@link InvocationStage} then allows the caller to add a callback that will be executed after the remaining
    * interceptors.</p>
    * <p>Note: {@code invokeNext(ctx, command)} does not throw exceptions. In order to handle exceptions from the
    * next interceptors, you <em>must</em> use one of the {@code InvocationStage} methods, e.g.
    * {@link InvocationStage#exceptionally(Function)},
    * {@link InvocationStage#handle(Object, TriFunction)}, or
    * {@link InvocationStage#whenComplete(Object, Object, TetraConsumer)}, or
    * {@link InvocationStage#compose(BiFunction)}</p>
    */
   public final InvocationStage invokeNext(InvocationContext ctx, VisitableCommand command) {
      try {
         if (nextDDInterceptor != null) {
            return (InvocationStage) command.acceptVisitor(ctx, nextDDInterceptor);
         } else {
            return nextInterceptor.visitCommand(ctx, command);
         }
      } catch (Throwable throwable) {
         return InvocationStageImpl.makeExceptional(throwable);
      }
   }

   /**
    * Suspend the invocation until {@code delay} completes, then if successful invoke the next interceptor.
    * <p>
    * If {@code delay} completes exceptionally, skip the next interceptor and continue with the exception.
    */
   public final InvocationStage invokeNextAsync(InvocationContext ctx, VisitableCommand command, CompletableFuture<?> delay) {
      if (delay.isDone() && !delay.isCompletedExceptionally()) {
         return invokeNext(ctx, command);
      } else {
         return InvocationStageImpl.makeAsynchronous(delay)
                                   .thenCompose(ctx, command, (rCtx, rCommand, rv) -> invokeNext(rCtx, rCommand));
      }
   }
}
