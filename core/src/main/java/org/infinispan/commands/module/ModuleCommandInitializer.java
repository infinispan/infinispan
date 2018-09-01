package org.infinispan.commands.module;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Modules which wish to implement their own commands and visitors must also provide an implementation of this
 * interface.
 *
 * @author Manik Surtani
 * @since 5.0
 */
@Scope(Scopes.NAMED_CACHE)
@SurvivesRestarts
public interface ModuleCommandInitializer {

   /**
    * Initializes a command constructed using {@link ModuleCommandFactory#fromStream(byte)} with
    * necessary named-cache-specific components.
    *
    * @param c command to initialize
    * @param isRemote true if the source is a remote node in the cluster, false otherwise.
    */
   void initializeReplicableCommand(ReplicableCommand c, boolean isRemote);
}
