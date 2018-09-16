package org.infinispan.remoting.inboundhandler.action;

/**
 * An action represents a step in {@link org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler}.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface Action {

   /**
    * It checks this action.
    * <p>
    * When {@link ActionStatus#READY} or {@link ActionStatus#CANCELED} are final states.
    * <p>
    * This method should be thread safe and idempotent since it can be invoked multiple times by multiples threads.
    *
    * @param state the current state.
    * @return the status of this action.
    */
   ActionStatus check(ActionState state);

   /**
    * Adds a listener to be invoked when this action is ready or canceled.
    *
    * @param listener the {@link ActionListener} to add.
    */
   default void addListener(ActionListener listener) {
   }

   /**
    * Invoked when an exception occurs while processing the command.
    *
    * @param state the current state.
    */
   default void onException(ActionState state) {
   }

   /**
    * Invoked always after the command is executed.
    *
    * @param state the current state.\
    */
   default void onFinally(ActionState state) {
   }
}
