package org.infinispan.context;

import org.infinispan.commands.DataCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.impl.ClearInvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.remoting.transport.Address;

/**
 * Base class for InvocationContextFactory implementations.
 *
 * @author Mircea Markus
 * @author Dan Berindei
 * @deprecated Since 9.0, this class is going to be moved to an internal package.
 */
@Deprecated
public abstract class AbstractInvocationContextFactory implements InvocationContextFactory {

   @Inject protected Configuration config;
   @Inject protected AsyncInterceptorChain interceptorChain;

   @Override
   public InvocationContext createRemoteInvocationContextForCommand(
         VisitableCommand cacheCommand, Address origin) {
      if (cacheCommand instanceof DataCommand && !(cacheCommand instanceof InvalidateCommand)) {
         return new SingleKeyNonTxInvocationContext(origin);
      } else if (cacheCommand instanceof PutMapCommand) {
         return new NonTxInvocationContext(((PutMapCommand) cacheCommand).getMap().size(), origin);
      } else if (cacheCommand instanceof ClearCommand) {
         return createClearInvocationContext(origin);
      } else {
         return createRemoteInvocationContext(origin);
      }
   }

   @Override
   public final InvocationContext createClearNonTxInvocationContext() {
      return createClearInvocationContext(null);
   }

   private ClearInvocationContext createClearInvocationContext(Address origin) {
      ClearInvocationContext context = new ClearInvocationContext(origin, interceptorChain);
      return context;
   }
}
