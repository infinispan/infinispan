package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

/**
 * An invocation stage that allows the interceptor to perform more actions after the remaining interceptors have
 * finished executing.
 *
 * <p>An instance can be obtained by calling one of the {@link BaseAsyncInterceptor} methods, e.g.
 * {@link BaseAsyncInterceptor#invokeNext(InvocationContext, VisitableCommand)}.</p>
 *
 * <p>The {@code InvocationStage} methods are intentionally very similar to the ones in
 * {@link java.util.concurrent.CompletionStage}. However, unlike {@link java.util.concurrent.CompletionStage},
 * adding a handler (e.g. with {@link #compose(InvocationComposeHandler)}) may return the same instance and
 * execute the handler synchronously, or modify the result of the current stage.
 * Therefore, saving a stage instance in a local variable/field and doing anything with it after adding a handler
 * is not allowed:</p>
 *
 * <pre>
 *    InvocationStage stage1 = ...;
 *
 *    // The next line may change the result of stage1 to "a"
 *    InvocationStage stage2 = stage1.thenApply((rCtx, rCommand, rv) -> "a");
 *
 *    CompletableFuture&lt;Object&gt; cf = new CompletableFuture&lt;&gt;();
 *    // The next line might block forever
 *    InvocationStage stage3 = stage1.thenApply((rCtx, rCommand, rv) -> cf.get());
 *    // If the stage is async, the next line will block instead
 *    System.out.println(stage1.get());
 * </pre>
 *
 * @author Dan Berindei
 * @since 9.0
 */
public interface InvocationStage extends BasicInvocationStage {

   /**
    * Execute a {@link InvocationComposeHandler} after the next interceptors and all the actions they added have
    * finished.
    * <p>
    * The handler will be executed even if there was an exception in the interceptors or in the handlers. It can return
    * any {@link BasicInvocationStage}, even by calling {@link BaseAsyncInterceptor} methods.
    */
   InvocationStage compose(InvocationComposeHandler composeHandler);

   InvocationStage thenCompose(InvocationComposeSuccessHandler thenComposeHandler);

   /**
    * Execute a {@link InvocationSuccessHandler} after the next interceptors and all the actions they added have
    * finished.
    * <p>
    * The handler will be executed only if the interceptors and their handlers were successful. It can not affect the
    * execution in any way, except by throwing an exception.
    */
   InvocationStage thenAccept(InvocationSuccessHandler successHandler);

   /**
    * Execute a {@link InvocationReturnValueHandler} after the next interceptors and all the actions they added have
    * finished.
    * <p>
    * The handler will be executed only if the interceptors and their handlers were successful. It can change the return
    * value, or it can throw an exception.
    */
   InvocationStage thenApply(InvocationReturnValueHandler returnValueHandler);

   /**
    * Execute a {@link InvocationExceptionHandler} after the next interceptors and all the actions they added have
    * finished.
    * <p>
    * The handler will be executed only if one of the interceptors or their handlers threw an exception, and another
    * handler didn't swallow the exception. It replace the existing exception with a new one by throwing, but it cannot
    * swallow it.
    */
   InvocationStage exceptionally(InvocationExceptionHandler exceptionHandler);

   /**
    * Execute a {@link InvocationFinallyHandler} after the next interceptors and all the actions they added have
    * finished.
    * <p>
    * The handler will be executed even if there was an exception in the interceptors or in the handlers. It can not
    * affect the execution in any way, except by throwing an exception.
    */
   InvocationStage handle(InvocationFinallyHandler finallyHandler);
}
