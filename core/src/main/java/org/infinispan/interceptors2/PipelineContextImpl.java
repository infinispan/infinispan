package org.infinispan.interceptors2;

import org.infinispan.context.InvocationContext;

import java.util.Arrays;

/**
 * @author Dan Berindei
 * @since 8.0
 */
public class PipelineContextImpl implements PipelineContext {
   private final InvocationContext ctx;
   private final InvocationStage[] interceptors;
   private int currentInterceptor = 0;

   public PipelineContextImpl(InvocationContext ctx, InvocationStage[] interceptors) {
      this.ctx = ctx;
      this.interceptors = interceptors;
   }

   @Override
   public InvocationContext getInvocationContext() {
      return ctx;
   }

   @Override
   public InvocationStage nextInterceptor() {
      if (currentInterceptor >= interceptors.length)
         return null;

      return interceptors[currentInterceptor++];
   }

   @Override
   public InvocationStage previousInterceptor() {
      if (currentInterceptor == 0)
         return null;

      return interceptors[currentInterceptor--];
   }

   @Override
   public PipelineContext getSubContext() {
      return new PipelineContextImpl(ctx, Arrays.copyOfRange(interceptors, currentInterceptor + 1, interceptors.length));
   }

   @Override
   public void pushValue(Object value) {
      // TODO
   }

   @Override
   public Object popValue() {
      return null;
   }
}
