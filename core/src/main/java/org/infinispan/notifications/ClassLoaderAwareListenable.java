package org.infinispan.notifications;

import java.util.concurrent.CompletionStage;

import org.infinispan.util.concurrent.CompletionStages;

/**
 * Interface that enhances {@link Listenable} with the possibility of specifying the
 * {@link ClassLoader} which should be set as the context class loader for the invoked
 * listener method
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface ClassLoaderAwareListenable extends Listenable {
   /**
    * Adds a listener along with a class loader to use for the invocation
    * @param listener listener to add, must not be null
    * @param classLoader classloader, must not be null
    */
   default void addListener(Object listener, ClassLoader classLoader) {
      CompletionStages.join(addListenerAsync(listener, classLoader));
   }

   /**
    * Asynchronous version of {@link #addListener(Object, ClassLoader)}
    * @param listener listener to add, must not be null
    * @param classLoader classloader, must not be null
    * @return CompletionStage that when complete the listener is fully installed
    */
   CompletionStage<Void> addListenerAsync(Object listener, ClassLoader classLoader);
}
