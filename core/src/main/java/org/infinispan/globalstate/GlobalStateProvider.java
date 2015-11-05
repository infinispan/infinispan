package org.infinispan.globalstate;

/**
 * GlobalStateProvider. Implementors who need to register with the {@link GlobalStateManager}
 * because they contribute to/are interested in the contents of the global persistent state.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public interface GlobalStateProvider {

   /**
    * This method is invoked by the {@link GlobalStateManager} just before
    * persisting the global state
    */
   void prepareForPersist(ScopedPersistentState globalState);

   /**
    * This method is invoked by the {@link GlobalStateManager} after starting up to notify
    * that global state has been restored.
    */
   void prepareForRestore(ScopedPersistentState globalState);
}
