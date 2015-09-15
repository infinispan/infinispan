package org.infinispan.interceptors2;

import java.util.concurrent.CompletableFuture;

/**
 * @author Dan Berindei
 * @since 8.0
 */
public interface InvocationPipeline {
   void addInterceptor(InvocationStage interceptor);

   CompletableFuture<Object> invoke(VisitableCommand2 command, PipelineContext ctx) throws Throwable;
}
