package org.infinispan.notifications;

/**
 * Interface that enhances {@link FilteringListenable} with the possibility of specifying the
 * {@link ClassLoader} which should be set as the context class loader for the invoked
 * listener method
 *
 * @author Manik Surtani
 * @since 6.0
 * @see ClassLoaderAwareListenable
 * @see FilteringListenable
 */
public interface ClassLoaderAwareFilteringListenable extends FilteringListenable {
   /**
    * Adds a listener to the component.  Typically, listeners would need to be annotated with {@link org.infinispan.notifications.Listener} and
    * further to that, contain methods annotated appropriately, otherwise the listener will not be registered.
    * <p/>
    * See the {@link org.infinispan.notifications.Listener} annotation for more information.
    * <p/>
    *
    * @param listener must not be null.
    * @param classLoader class loader
    */
   void addListener(Object listener, KeyFilter filter, ClassLoader classLoader);
}
