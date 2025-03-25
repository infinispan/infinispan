package org.infinispan.commands.remote;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The {@link org.infinispan.remoting.rpc.RpcManager} only replicates commands wrapped in a {@link CacheRpcCommand}.
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface CacheRpcCommand extends ReplicableCommand {

   static final Log log = LogFactory.getLog(CacheRpcCommand.class);

   /**
    * Invoke the command asynchronously.
    * <p>This method replaces {@link #invoke()} for remote execution.
    * The default implementation and {@link #invoke()} will be removed in future versions.
    * </p>
    *
    * @since 11.0
    */
   default CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      if (getCacheName() != null && this instanceof VisitableCommand cmd) {
         cmd.init(registry);
         InvocationContextFactory icf = registry.getInvocationContextFactory().running();
         InvocationContext ctx = icf.createRemoteInvocationContextForCommand(cmd, getOrigin());
         if (cmd instanceof RemoteLockCommand remoteLockCmd) {
            ctx.setLockOwner(remoteLockCmd.getKeyLockOwner());
         }
         if (log.isTraceEnabled())
            log.tracef("Invoking command %s, with originLocal flag set to %b", cmd, ctx.isOriginLocal());
         return registry.getInterceptorChain().running().invokeAsync(ctx, cmd);
      }
      return invokeAsync();
   }

   /**
    * @return the name of the cache that produced this command.  This will also be the name of the cache this command is
    *         intended for.
    */
   ByteString getCacheName();

   default void setCacheName(ByteString cacheName) {
      if (!getCacheName().equals(cacheName))
         throw new IllegalStateException("setCacheName must be overridden if cacheName is mutable");
   }

   /**
    * Set the origin of the command
    */
   void setOrigin(Address origin);

   /**
    * Get the origin of the command
    */
   Address getOrigin();
}
