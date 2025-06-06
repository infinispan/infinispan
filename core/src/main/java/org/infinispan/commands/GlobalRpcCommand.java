package org.infinispan.commands;

import java.util.concurrent.CompletionStage;

import org.infinispan.factories.GlobalComponentRegistry;

/**
 * Commands correspond to specific areas of functionality in the cluster, and can be replicated using the {@link
 * org.infinispan.remoting.inboundhandler.GlobalInboundInvocationHandler}.
 *
 * Implementations of this interface must not rely on calls to {@link GlobalComponentRegistry#wireDependencies(Object)},
 * as {@code @Inject} annotations on implementations will be ignored, components must be accessed via the
 * {@link GlobalComponentRegistry} parameter of {@link #invokeAsync(GlobalComponentRegistry)}.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public interface GlobalRpcCommand extends ReplicableCommand {
   /**
    * Invoke the command asynchronously.
    */
   CompletionStage<?> invokeAsync(GlobalComponentRegistry globalComponentRegistry) throws Throwable;
}
