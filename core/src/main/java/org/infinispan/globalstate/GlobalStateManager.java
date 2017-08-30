package org.infinispan.globalstate;

import java.util.Optional;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * GlobalStateManager.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@Scope(Scopes.GLOBAL)
public interface GlobalStateManager {
   /**
    * Registers a state provider within this state manager
    *
    * @param provider
    */
   void registerStateProvider(GlobalStateProvider provider);

   /**
    * Reads the persistent state for the specified scope.
    */
   Optional<ScopedPersistentState> readScopedState(String scope);

   /**
    * Persists the specified scoped state
    */
   void writeScopedState(ScopedPersistentState state);

   /**
    * Persists the global state by contacting all registered scope providers
    */
   void writeGlobalState();
}
