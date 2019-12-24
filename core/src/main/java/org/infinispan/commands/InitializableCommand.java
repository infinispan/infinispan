package org.infinispan.commands;

import org.infinispan.factories.ComponentRegistry;

/**
 * An interface to be implemented by Commands which require an intialized state after deserialization.
 *
 * @author Ryan Emerson
 * @since 10.0
 * @deprecated since 11.0, please implement {@link ReplicableCommand#invokeAsync(ComponentRegistry)} instead
 */
@Deprecated
public interface InitializableCommand {

   void init(ComponentRegistry componentRegistry, boolean isRemote);

}
