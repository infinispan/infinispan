package org.infinispan.commands.remote;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;

/**
 * Base class for RPC commands.
 *
 * @author Mircea.Markus@jboss.com
 */
public abstract class BaseRpcCommand implements CacheRpcCommand {

   protected InterceptorChain interceptorChain;
   protected InvocationContextContainer icc;
   protected String cacheName;

   private static final Log log = LogFactory.getLog(BaseRpcCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   protected BaseRpcCommand(String cacheName) {
      this.cacheName = cacheName;
   }

   BaseRpcCommand() {
   }

   public String getCacheName() {
      return cacheName;
   }

   public void init(InterceptorChain interceptorChain, InvocationContextContainer icc) {
      this.interceptorChain = interceptorChain;
      this.icc = icc;
   }

   protected final Object processVisitableCommand(ReplicableCommand cacheCommand) throws Throwable {
      if (cacheCommand instanceof VisitableCommand) {
         InvocationContext ctx = icc.getRemoteNonTxInvocationContext();
         if (trace) log.trace("Invoking command " + cacheCommand + ", with originLocal flag set to " + ctx.isOriginLocal() + ".");
         return interceptorChain.invoke(ctx, (VisitableCommand) cacheCommand);
         // we only need to return values for a set of remote calls; not every call.
      } else {
         throw new RuntimeException("Do we still need to deal with non-visitable commands? (" + cacheCommand.getClass().getName() + ")");
      }
   }
}
