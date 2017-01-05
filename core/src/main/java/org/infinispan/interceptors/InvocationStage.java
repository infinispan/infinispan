package org.infinispan.interceptors;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.function.TetraConsumer;
import org.infinispan.util.function.TetraFunction;
import org.infinispan.util.function.TriConsumer;
import org.infinispan.util.function.TriFunction;

/**
 * An invocation stage that allows the interceptor to perform more actions after the remaining interceptors have
 * finished executing.
 *
 * <p>An instance can be obtained by calling one of the {@link BaseAsyncInterceptor} methods, e.g.
 * {@link BaseAsyncInterceptor#invokeNext(InvocationContext, VisitableCommand)}.</p>
 *
 * <p>The {@code InvocationStage} methods are intentionally very similar to the ones in
 * {@link java.util.concurrent.CompletionStage}. However, unlike {@link java.util.concurrent.CompletionStage},
 * adding a handler (e.g. with {@link #compose(BiFunction)})
 * may return the same instance and execute the handler synchronously, or modify the result of the current stage.
 * Therefore, saving a stage instance in a local variable/field and doing anything with it after adding a handler
 * is not allowed:</p>
 *
 * <pre>
 *    InvocationStage stage1 = ...;
 *
 *    // The next line may change the result of stage1 to "a"
 *    InvocationStage stage2 = stage1.thenApply(ctx, command, (rCtx, rCommand, rv) -> "a");
 *
 *    CompletableFuture&lt;Object&gt; cf = new CompletableFuture&lt;&gt;();
 *    // The next line might block forever
 *    InvocationStage stage3 = stage1.thenApply(ctx, command, (rCtx, rCommand, rv) -> cf.get());
 *    // If the stage is async, the next line will block instead
 *    System.out.println(stage1.get());
 * </pre>
 *
 * <p>{@code InvocationStage} also allows you to externalize the lambda's state, so that the lambda never captures
 * any local variables. This is useful in Infinispan because the overhead of allocating new lambdas in each interceptor
 * would be prohibitive for very fast operations.</p>
 *
 * @author Dan Berindei
 * @since 9.0
 */
public interface InvocationStage {
   /**
    * @return the result of the invocation stage.
    * @throws Throwable If the invocation stage threw an exception.
    */
   Object get() throws Throwable;

   /**
    * @return {@code true} if the invocation is completed, {@code false} otherwise.
    */
   boolean isDone();

   /**
    * Convert to {@link CompletableFuture}, used internally by the asynchronous API.
    */
   CompletableFuture<Object> toCompletableFuture();


   // Methods with 0 external parameters

   /**
    * Execute a callback after stage has finished successfully.
    * <p>
    * It can change the return value, or it can throw an exception.
    */
   InvocationStage thenApply(Function<Object, Object> function);

   /**
    * Execute a callback after the stage has finished successfully.
    * <p>
    * The callback can not change the return value, but it can throw an exception.
    */
   InvocationStage thenAccept(Consumer<Object> action);

   /**
    * Execute a callback after the stage has finished successfully.
    * <p>
    * The callback can return any {@link InvocationStage}, e.g. using
    * {@link BaseAsyncInterceptor#invokeNext(InvocationContext, VisitableCommand)} to invoke a new command.
    */
   InvocationStage thenCompose(Function<Object, InvocationStage> function);

   /**
    * Execute a callback after this stage and the {@code CompletionStage} parameter both completed successfully.
    *
    * The callback is a {@code BiFunction}, with the result of this stage as the first parameter and the result of the
    * {@code completionStage} parameter as the second.
    */
   InvocationStage thenCombine(CompletionStage<?> otherStage, BiFunction<Object, Object, Object> function);

   /**
    * Execute a callback after this stage and the {@code InvocationStage} parameter both completed successfully.
    *
    * The callback is a {@code BiFunction}, with the result of this stage as the first parameter and the result of the
    * {@code completionStage} parameter as the second.
    */
   InvocationStage thenCombine(InvocationStage otherStage, BiFunction<Object, Object, Object> function);

   /**
    * Execute a callback after the stage has finished with an exception.
    * <p>
    * The callback can rethrow the exception, throw a new one, or return a regular value.
    */
   InvocationStage exceptionally(Function<Throwable, Object> function);

   /**
    * Execute a callback after the stage has finished with an exception.
    * <p>
    * The callback can rethrow the exception, throw a new one, or return another {@link InvocationStage}, e.g.
    * using {@link BaseAsyncInterceptor#invokeNext(InvocationContext, VisitableCommand)} to invoke a new command.
    */
   InvocationStage exceptionallyCompose(Function<Throwable, InvocationStage> function);

   /**
    * Execute a callback after the stage has finished (either successfully or with an exception).
    * <p>
    * The callback can not change the return value, but it can throw an exception.
    */
   InvocationStage handle(BiFunction<Object, Throwable, Object> function);

   /**
    * Execute a callback after the stage has finished (either successfully or with an exception).
    * <p>
    * The callback can rethrow the exception, throw a new one, or return a regular value.
    */
   InvocationStage whenComplete(BiConsumer<Object, Throwable> action);

   /**
    * Execute a callback after the stage has finished (either successfully or with an exception).
    * <p>
    * The callback can return any {@link InvocationStage}, e.g. using
    * {@link BaseAsyncInterceptor#invokeNext(InvocationContext, VisitableCommand)} to invoke a new command.
    */
   InvocationStage compose(BiFunction<Object, Throwable, InvocationStage> function);


   // Methods with 1 external parameter

   /**
    * Execute a callback after stage has finished successfully.
    * <p>
    * It can change the return value, or it can throw an exception.
    */
   <P1> InvocationStage thenApply(P1 p1, BiFunction<P1, Object, Object> function);

   /**
    * Execute a callback after the stage has finished successfully.
    * <p>
    * The callback can not change the return value, but it can throw an exception.
    */
   <P1> InvocationStage thenAccept(P1 p1, BiConsumer<P1, Object> action);

   /**
    * Execute a callback after the stage has finished successfully.
    * <p>
    * The callback can return any {@link InvocationStage}, e.g. using
    * {@link BaseAsyncInterceptor#invokeNext(InvocationContext, VisitableCommand)} to invoke a new command.
    */
   <P1> InvocationStage thenCompose(P1 p1, BiFunction<P1, Object, InvocationStage> function);

   /**
    * Execute a callback after the stage has finished with an exception.
    * <p>
    * The callback can rethrow the exception, throw a new one, or return a regular value.
    */
   <P1> InvocationStage exceptionally(P1 p1, BiFunction<P1, Throwable, Object> function);

   /**
    * Execute a callback after the stage has finished with an exception.
    * <p>
    * The callback can rethrow the exception, throw a new one, or return another {@link InvocationStage}, e.g.
    * using {@link BaseAsyncInterceptor#invokeNext(InvocationContext, VisitableCommand)} to invoke a new command.
    */
   <P1> InvocationStage exceptionallyCompose(P1 p1, BiFunction<P1, Throwable, InvocationStage> function);

   /**
    * Execute a callback after the stage has finished (P1 p1, either successfully or with an exception).
    * <p>
    * The callback can not change the return value, but it can throw an exception.
    */
   <P1> InvocationStage handle(P1 p1, TriFunction<P1, Object, Throwable, Object> function);

   /**
    * Execute a callback after the stage has finished (P1 p1, either successfully or with an exception).
    * <p>
    * The callback can rethrow the exception, throw a new one, or return a regular value.
    */
   <P1> InvocationStage whenComplete(P1 p1, TriConsumer<P1, Object, Throwable> action);

   /**
    * Execute a callback after the stage has finished (P1 p1, either successfully or with an exception).
    * <p>
    * The callback can return any {@link InvocationStage}, e.g. using
    * {@link BaseAsyncInterceptor#invokeNext(InvocationContext, VisitableCommand)} to invoke a new command.
    */
   <P1> InvocationStage compose(P1 p1, TriFunction<P1, Object, Throwable, InvocationStage> function);


   // Methods with 2 external parameters

   /**
    * Execute a callback after stage has finished successfully.
    * <p>
    * It can change the return value, or it can throw an exception.
    */
   <P1, P2> InvocationStage thenApply(P1 p1, P2 p2, TriFunction<P1, P2, Object, Object> function);

   /**
    * Execute a callback after the stage has finished successfully.
    * <p>
    * The callback can not change the return value, but it can throw an exception.
    */
   <P1, P2> InvocationStage thenAccept(P1 p1, P2 p2, TriConsumer<P1, P2, Object> action);

   /**
    * Execute a callback after the stage has finished successfully.
    * <p>
    * The callback can return any {@link InvocationStage}, e.g. using
    * {@link BaseAsyncInterceptor#invokeNext(InvocationContext, VisitableCommand)} to invoke a new command.
    */
   <P1, P2> InvocationStage thenCompose(P1 p1, P2 p2, TriFunction<P1, P2, Object, InvocationStage> function);

   /**
    * Execute a callback after the stage has finished with an exception.
    * <p>
    * The callback can rethrow the exception, throw a new one, or return a regular value.
    */
   <P1, P2> InvocationStage exceptionally(P1 p1, P2 p2, TriFunction<P1, P2, Throwable, Object> function);

   /**
    * Execute a callback after the stage has finished with an exception.
    * <p>
    * The callback can rethrow the exception, throw a new one, or return another {@link InvocationStage}, e.g.
    * using {@link BaseAsyncInterceptor#invokeNext(InvocationContext, VisitableCommand)} to invoke a new command.
    */
   <P1, P2> InvocationStage exceptionallyCompose(P1 p1, P2 p2,
                                                 TriFunction<P1, P2, Throwable, InvocationStage> function);

   /**
    * Execute a callback after the stage has finished (P1 p1, P2 p2, either successfully or with an exception).
    * <p>
    * The callback can not change the return value, but it can throw an exception.
    */
   <P1, P2> InvocationStage handle(P1 p1, P2 p2, TetraFunction<P1, P2, Object, Throwable, Object> function);

   /**
    * Execute a callback after the stage has finished (P1 p1, P2 p2, either successfully or with an exception).
    * <p>
    * The callback can rethrow the exception, throw a new one, or return a regular value.
    */
   <P1, P2> InvocationStage whenComplete(P1 p1, P2 p2, TetraConsumer<P1, P2, Object, Throwable> action);

   /**
    * Execute a callback after the stage has finished (P1 p1, P2 p2, either successfully or with an exception).
    * <p>
    * The callback can return any {@link InvocationStage}, e.g. using
    * {@link BaseAsyncInterceptor#invokeNext(InvocationContext, VisitableCommand)} to invoke a new command.
    */
   <P1, P2> InvocationStage compose(P1 p1, P2 p2, TetraFunction<P1, P2, Object, Throwable, InvocationStage> function);
}
