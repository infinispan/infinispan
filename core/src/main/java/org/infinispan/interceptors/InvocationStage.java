package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

/**
 * An invocation stage that allows the interceptor to perform more actions after the remaining interceptors have
 * finished executing.
 * <p>
 * It can also be returned directly from {@link AsyncInterceptor#visitCommand(InvocationContext, VisitableCommand)}.
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
