package org.infinispan.notifications;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.util.concurrent.CompletionStages;

/**
 * Interface that denotes that the implementation can have listeners attached to it.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface Listenable {

   /**
    * Adds a listener to the component.  Typically, listeners would need to be annotated with {@link Listener} and
    * further to that, contain methods annotated appropriately, otherwise the listener will not be registered.
    * <p/>
    * See the {@link Listener} annotation for more information.
    * <p/>
    *
    * @param listener listener to add, must not be null
    */
   default void addListener(Object listener) {
      CompletionStages.join(addListenerAsync(listener));
   }

   /**
    * Asynchronous version of {@link #addListener(Object)}
    * @param listener listener to add, must not be null
    * @return CompletionStage that when complete the listener is fully installed
    */
   CompletionStage<Void> addListenerAsync(Object listener);

   /**
    * Removes a listener from the component.
    *
    * @param listener listener to remove.  Must not be null.
    * @throws org.infinispan.IllegalLifecycleStateException may be thrown if the {@code Listenable} is stopped.
    */
   default void removeListener(Object listener) {
      CompletionStages.join(removeListenerAsync(listener));
   }

   /**
    * Asynchronous version of {@link #removeListener(Object)}
    * @param listener listener to remove, must not be null
    * @return CompletionStage that when complete the listener is fully removed
    */
   CompletionStage<Void> removeListenerAsync(Object listener);

   /**
    * @return a set of all listeners registered on this component.
    */
   Set<Object> getListeners();
}
