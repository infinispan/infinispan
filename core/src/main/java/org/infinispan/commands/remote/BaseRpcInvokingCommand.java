package org.infinispan.commands.remote;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Base class for RPC commands.
 *
 * @author Mircea.Markus@jboss.com
 */
public abstract class BaseRpcInvokingCommand extends BaseRpcCommand {

   protected InterceptorChain interceptorChain;
   protected InvocationContextContainer icc;

   private static final Log log = LogFactory.getLog(BaseRpcInvokingCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   protected BaseRpcInvokingCommand(String cacheName) {
      super(cacheName);
   }

   BaseRpcInvokingCommand() {
   }

   public void init(InterceptorChain interceptorChain, InvocationContextContainer icc) {
      this.interceptorChain = interceptorChain;
      this.icc = icc;
   }

   protected final Object processVisitableCommand(ReplicableCommand cacheCommand) throws Throwable {
      if (cacheCommand instanceof VisitableCommand) {
         InvocationContext ctx = icc.createRemoteInvocationContext();
         VisitableCommand vc = (VisitableCommand) cacheCommand;
         if (vc.shouldInvoke(ctx)) {
            if (trace) log.trace("Invoking command " + cacheCommand + ", with originLocal flag set to " + ctx.isOriginLocal() + ".");
            return interceptorChain.invoke(ctx, vc);
         } else {
            if (trace) log.trace("Not invoking command " + cacheCommand + " since shouldInvoke() returned false with context " + ctx);
            return null;
         }
         // we only need to return values for a set of remote calls; not every call.
      } else {
         throw new RuntimeException("Do we still need to deal with non-visitable commands? (" + cacheCommand.getClass().getName() + ")");
      }
   }
}
