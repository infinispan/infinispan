package org.infinispan.commands.remote;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.logging.Log;
import org.infinispan.logging.LogFactory;

/**
 * Base class for RPC commands.
 *
 * @author Mircea.Markus@jboss.com
 */
public abstract class BaseRpcCommand implements CacheRpcCommand {

   protected InterceptorChain interceptorChain;
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

   public void setInterceptorChain(InterceptorChain interceptorChain) {
      this.interceptorChain = interceptorChain;
   }


   protected Object processCommand(InvocationContext ctx, ReplicableCommand cacheCommand) throws Throwable {
      Object result;
      try {
         if (trace) log.trace("Invoking command " + cacheCommand + ", with originLocal flag set to false.");
         ctx.setOriginLocal(false);
         if (cacheCommand instanceof VisitableCommand) {
            Object retVal = interceptorChain.invokeRemote((VisitableCommand) cacheCommand);
            // we only need to return values for a set of remote calls; not every call.
            result = null;
         } else {
            throw new RuntimeException("Do we still need to deal with non-visitable commands? (" + cacheCommand.getClass().getName() + ")");
//            result = cacheCommand.perform(null);
         }
      }
      catch (Throwable ex) {
         // TODO deal with PFER
//         if (!(cacheCommand instanceof PutForExternalReadCommand))
//         {
         throw ex;
//         }
//         else
//         {
//            if (trace)
//               log.trace("Caught an exception, but since this is a putForExternalRead() call, suppressing the exception.  Exception is:", ex);
//            result = null;
//         }
      }
      return result;
   }
}
