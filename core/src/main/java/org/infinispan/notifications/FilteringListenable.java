package org.infinispan.notifications;

/**
 * A Listable that can also filter events based on key
 *
 * @author Manik Surtani
 * @since 6.0
 */
public interface FilteringListenable extends Listenable {
   /**
    * Adds a listener to the component.  Typically, listeners would need to be annotated with {@link org.infinispan.notifications.Listener} and
    * further to that, contain methods annotated appropriately, otherwise the listener will not be registered.
    * <p/>
    * See the {@link org.infinispan.notifications.Listener} annotation for more information.
    * <p/>
    *
    * @param listener must not be null.
    */
   void addListener(Object listener, KeyFilter filter);
}
