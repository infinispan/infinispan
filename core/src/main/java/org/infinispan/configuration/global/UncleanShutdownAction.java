package org.infinispan.configuration.global;

/**
 * Defines the action taken when a dangling lock file is found in the persistent global state, signifying an
 * unclean shutdown of the node (usually because of a crash or an external termination).
 *
 * @since 15.0
 */
public enum UncleanShutdownAction {
   /**
    * Prevents startup of the cache manager if a dangling lock file is found in the persistent global state.
    */
   FAIL,
   /**
    * Clears the persistent global state if a dangling lock file is found in the persistent global state.
    */
   PURGE,
   /**
    * Ignores the presence of a dangling lock file in the persistent global state.
    */
   IGNORE
}
