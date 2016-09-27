package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.util.Experimental;
import org.infinispan.context.InvocationContext;

/**
 * Interface for sequential interceptors.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@Experimental
public interface AsyncInterceptor {
   /**
    * Perform some work for a command invocation.
    *
    * The interceptor is responsible for invoking the next interceptor in the chain.
    * See {@link BaseAsyncInterceptor} and {@link InvocationStage} for the ways to do that.
    */
   BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable;

   /**
    * Sets up the interceptor. Do not call explicitly.
    */
   void setNextInterceptor(AsyncInterceptor interceptorStage);
}
