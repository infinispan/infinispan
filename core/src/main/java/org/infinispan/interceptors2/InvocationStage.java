package org.infinispan.interceptors2;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * @author Dan Berindei
 * @since 8.0
 */
public interface InvocationStage {
   Object CONTINUE = new Object();

   /**
    * Perform some kind of action before the command is performed.
    *
    * Return {@code null} to continue with the next interceptor, non-{@code null} to delay the next
    * interceptor until the {@code CompletableFuture} is done.
    *
    * The invocation is interrupted if the interceptor throws an exception, if the future completes
    * with an exception, or if the future completes with {@link #CONTINUE}.
    */
   CompletableFuture<Object> beforeCommand(PipelineContext ctx, VisitableCommand2 cmd) throws Throwable;
   CompletableFuture<Object> afterCommand(PipelineContext ctx, VisitableCommand2 cmd, Object result, Throwable exception);
}
