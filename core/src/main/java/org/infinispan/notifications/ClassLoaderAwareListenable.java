package org.infinispan.notifications;

/**
 * Interface that enhances {@link Listenable} with the possibility of specifying the
 * {@link ClassLoader} which should be set as the context class loader for the invoked
 * listener method
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface ClassLoaderAwareListenable extends Listenable {
   public void addListener(Object listener, ClassLoader classLoader);
}
