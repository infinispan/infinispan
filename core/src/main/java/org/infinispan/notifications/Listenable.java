package org.infinispan.notifications;

import java.util.Set;

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
    * @param listener must not be null.
    */
   void addListener(Object listener);

   /**
    * Removes a listener from the component.
    *
    * @param listener listener to remove.  Must not be null.
    * @throws org.infinispan.IllegalLifecycleStateException may be thrown if the {@code Listenable} is stopped.
    */
   void removeListener(Object listener);

   /**
    * @return a set of all listeners registered on this component.
    */
   Set<Object> getListeners();
}
