package org.infinispan.interceptors2;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * @author Dan Berindei
 * @since 8.0
 */
public class InvocationPipelineImpl implements InvocationPipeline {
   public List<InvocationStage> interceptors;

   @Override
   public void addInterceptor(InvocationStage interceptor) {
      interceptors.add(interceptor);
   }

   @Override
   public CompletableFuture<Object> invoke(VisitableCommand2 command, PipelineContext ctx) throws Throwable {
      InvocationStage interceptor;
      while ((interceptor = ctx.nextInterceptor()) != null){
         CompletableFuture<Object> future = interceptor.beforeCommand(ctx, command);
         if (future == null) {
            // No asynchronous stuff, just continue with the next interceptor
            continue;
         }

         if (future.isDone()) {
            Object result = future.join();
            if (result == InvocationStage.CONTINUE) {
               // If empty, continue the loop with the next interceptor
               continue;
            }
            return invokeAfter(command, ctx, result, null);
         }

         return future.handle((result, throwable) -> {
            if (throwable != null || result != InvocationStage.CONTINUE) {
               return invokeAfter(command, ctx, result, throwable);
            }
            try {
               return invoke(command, ctx);
            } catch (Throwable throwable1) {
               throwable.addSuppressed(throwable1);
               throw new CompletionException(throwable);
            }
         });
      }

      Object result = command.perform(ctx.getInvocationContext());

      return invokeAfter(command, ctx, result, null);
   }

   private CompletableFuture<Object> invokeAfter(VisitableCommand2 command, PipelineContext ctx, Object initialResult,
                                                 Throwable exception) {
      Object result = initialResult;
      InvocationStage interceptor;
      while ((interceptor = ctx.previousInterceptor()) != null) {
         CompletableFuture<Object> future = interceptor.afterCommand(ctx, command, result, exception);
         if (future == null) {
            // No asynchronous stuff, just continue with the previous interceptor
            // And keep the result unchanged
            continue;
         }
         if (future.isDone()) {
            result = future.join();
            continue;
         }
         return future.handle((newResult, throwable) -> {
            ctx.previousInterceptor();
            return invokeAfter(command, ctx, newResult, throwable);
         });
      }

      return CompletableFuture.completedFuture(result);
   }
}
