package org.infinispan.interceptors2;

import org.infinispan.context.InvocationContext;

/**
 * @author Dan Berindei
 * @since 8.0
 */
public interface PipelineContext {
   InvocationContext getInvocationContext();
   InvocationStage nextInterceptor();
   InvocationStage previousInterceptor();
   PipelineContext getSubContext();

   void pushValue(Object value);
   Object popValue();
}
