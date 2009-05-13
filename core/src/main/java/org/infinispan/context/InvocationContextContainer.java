package org.infinispan.context;

import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.factories.annotations.NonVolatile;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Manages the association between an {@link org.infinispan.context.InvocationContext} and the calling thread. Also acts
 * as a factory for creating various types of {@link org.infinispan.context.InvocationContext}s.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@NonVolatile
@Scope(Scopes.NAMED_CACHE)
public interface InvocationContextContainer {

   /**
    * If we are in a tx scope this will return an {@link org.infinispan.context.impl.TxInvocationContext}. Otherwise it
    * will return an {@link org.infinispan.context.impl.NonTxInvocationContext}. Either way, both context will be marked
    * as local, i.e. {@link InvocationContext#isOriginLocal()} will be true. The context is also associated with the
    * current thread, so further calls to {@link #getThreadContext()} will return same instace.
    */
   InvocationContext getLocalInvocationContext();

   /**
    * Returns a {@link org.infinispan.context.impl.LocalTxInvocationContext}. The context is also associated with the
    * current thread, so further calls to {@link #getThreadContext()} will return same instace.
    */
   LocalTxInvocationContext getLocalTxInvocationContext();

   /**
    * Returns an {@link org.infinispan.context.impl.RemoteTxInvocationContext}. The context is also associated with the
    * current thread, so further calls to {@link #getThreadContext()} will return same instace.
    */
   RemoteTxInvocationContext getRemoteTxInvocationContext();

   /**
    * Returns an {@link org.infinispan.context.impl.NonTxInvocationContext} whose {@link
    * org.infinispan.context.impl.NonTxInvocationContext#isOriginLocal()} flag will be true. The context is also
    * associated with the current thread, so further calls to {@link #getThreadContext()} will return same instace.
    */
   InvocationContext getRemoteNonTxInvocationContext();

   /**
    * Returns the {@link InvocationContext}  that is currently associated with the calling thread. Important:
    * implementations of this metrhod is most likely expensive (ThreadLocal.get), it is recommanded to cache the result
    * of this method rather than repeting the call.
    *
    * @throws IllegalStateException if there is no context associated with the current thread.
    */
   InvocationContext getThreadContext();

   /**
    * Dissasociates thread's invocation context and returns the existing value.
    */
   InvocationContext suspend();

   /**
    * Associates the supplied {@link InvocationContext} with the calling thread.
    */
   void resume(InvocationContext ic);
}
