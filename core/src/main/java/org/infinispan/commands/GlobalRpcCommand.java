package org.infinispan.commands;

import java.util.concurrent.CompletionStage;

import org.infinispan.factories.GlobalComponentRegistry;

/**
 * Commands correspond to specific areas of functionality in the cluster, and can be replicated using the
 * {@link org.infinispan.remoting.inboundhandler.GlobalInboundInvocationHandler}.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public interface GlobalRpcCommand extends ReplicableCommand {
   /**
    * Invoke the command asynchronously.
    */
   default CompletionStage<?> invokeAsync(GlobalComponentRegistry globalComponentRegistry) throws Throwable {
      return invokeAsync();
   }
}
