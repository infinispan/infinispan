package org.infinispan.interceptors;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.util.Experimental;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.impl.AbstractInvocationStage;
import org.infinispan.interceptors.impl.AsyncInvocationStage;
import org.infinispan.interceptors.impl.ComposedAsyncInvocationStage;
import org.infinispan.interceptors.impl.ExceptionStage;
import org.infinispan.interceptors.impl.ReturnValueStage;

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
   }

   /**
    * Invoke the next interceptor, possibly with a new command.
    *
    * <p>{@link InvocationStage} then allows the caller to add a callback that will be executed after the remaining
    * interceptors.</p>
    * <p>Note: {@code invokeNext(ctx, command)} does not throw exceptions. In order to handle exceptions from the
    * next interceptors, you <em>must</em> use {@link InvocationStage#exceptionally(InvocationExceptionHandler)},
    * {@link InvocationStage#handle(InvocationFinallyHandler)}, or {@link InvocationStage#compose(InvocationComposeHandler)}</p>
    */
   public InvocationStage invokeNext(InvocationContext ctx, VisitableCommand command) {
      try {
         BasicInvocationStage stage = nextInterceptor.visitCommand(ctx, command);
         return stage.toInvocationStage(ctx, command);
       } catch (Throwable throwable) {
         return new ExceptionStage(ctx, command, throwable);
      }
   }

   /**
    * Return a value directly, skipping the remaining interceptors in the chain.
    */
   public BasicInvocationStage returnWith(Object returnValue) {
      return new ReturnValueStage(null, null, returnValue);
   }

   /**
    * Suspend the invocation until {@code stageFuture} completes, then evaluate its result and skip the remaining
    * interceptors.
    * <p>
    * The caller is supposed to call the
    */
   public InvocationStage goAsync(CompletableFuture<BasicInvocationStage> stageFuture) {
      if (stageFuture.isDone()) {
         InvocationStage stage;
         try {
            stage = ((AbstractInvocationStage) stageFuture.join());
         } catch (Throwable t) {
            stage = new ExceptionStage(null, null, extractException(t));
         }
         return stage;
      }

      return new ComposedAsyncInvocationStage(null, null, stageFuture);
   }

   /**
    * Suspend the invocation until {@code delay} completes, then if successful invoke the next interceptor.
    * <p>
    * If {@code delay} completes exceptionally, skip the next interceptor and continue with the exception.
    */
   public InvocationStage invokeNextAsync(InvocationContext ctx, VisitableCommand command, CompletableFuture<?> delay) {
      if (delay.isDone()) {
         InvocationStage stage;
         try {
            // Make sure the delay was successful
            delay.join();
            stage = invokeNext(ctx, command);
         } catch (Throwable t) {
            stage = new ExceptionStage(ctx, command, extractException(t));
         }
         return stage;
      }

      CompletableFuture<BasicInvocationStage> stageFuture = delay.thenApply(o -> invokeNext(ctx, command));
      return new ComposedAsyncInvocationStage(ctx, command, stageFuture);
   }

   /**
    * Suspend the invocation until {@code returnValueFuture} completes, then return its result and skip the remaining
    * interceptors.
    * <p>
    * The caller can continue invoking the next interceptor, e.g.
    * {@code goAsync2(ctx, command, v).thenApply((rCtx, rCommand, rv, t) -> invokeNext(rCtx, rCommand))}
    */
   public BasicInvocationStage returnWithAsync(CompletableFuture<Object> valueFuture) {
      if (valueFuture.isDone()) {
         InvocationStage stage;
         try {
            Object value = valueFuture.join();
            stage = new ReturnValueStage(null, null, value);
         } catch (Throwable t) {
            stage = new ExceptionStage(null, null, extractException(t));
         }
         return stage;
      }

      return new AsyncInvocationStage(null, null, valueFuture);
   }

   private Throwable extractException(Throwable t) {
      if (t.getCause() != null && t instanceof CompletionException) {
         t = t.getCause();
      }
      return t;
   }
}
