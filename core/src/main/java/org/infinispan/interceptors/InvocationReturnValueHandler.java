package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.impl.ExceptionStage;
import org.infinispan.interceptors.impl.ReturnValueStage;

/**
 * Callback interface for {@link InvocationStage#thenApply(InvocationReturnValueHandler)}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@FunctionalInterface
public interface InvocationReturnValueHandler extends InvocationComposeHandler {
   Object apply(InvocationContext rCtx, VisitableCommand rCommand, Object rv)
         throws Throwable;

   @Override
   default BasicInvocationStage apply(BasicInvocationStage stage, InvocationContext rCtx, VisitableCommand rCommand, Object rv,
         Throwable t) throws Throwable {
      if (t != null) return stage;

      try {
         Object result = apply(rCtx, rCommand, rv);
         return new ReturnValueStage(rCtx, rCommand, result);
      } catch (Throwable t1) {
         return new ExceptionStage(rCtx, rCommand, t1);
      }
   }
}
