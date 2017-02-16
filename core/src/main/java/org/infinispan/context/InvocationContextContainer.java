package org.infinispan.context;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Manages the association between an {@link org.infinispan.context.InvocationContext} and the calling thread.
 *
 * @author Manik Surtani (manik AT infinispan DOT org)
 * @author Mircea.Markus@jboss.com
 * @deprecated Since 9.0, this interface is going to be moved to an internal package.
 */
@Deprecated
@Scope(Scopes.GLOBAL)
public interface InvocationContextContainer {
   /**
    * Returns the {@link InvocationContext} that is currently associated with the calling thread. Important:
    * implementations of this method are most likely expensive, involving thread locals. It is recommended to cache
    * the result of this method rather than repeating the call.
    *
    * @throws IllegalStateException if there is no context associated with the current thread.
    *
    * @param quiet
    */
   InvocationContext getInvocationContext(boolean quiet);

   /**
    * Associate the InvocationContext parameter with the calling thread.
    */
   void setThreadLocal(InvocationContext context);

   /**
    * Remove the stored InvocationContext from the calling thread.
    *
    * Must be called as each thread exists the interceptor chain.
    */
   void clearThreadLocal();

   default void clearThreadLocal(InvocationContext context) {
      clearThreadLocal();
   }
}
