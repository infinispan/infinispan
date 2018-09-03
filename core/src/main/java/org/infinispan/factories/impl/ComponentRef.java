package org.infinispan.factories.impl;

/**
 * Reference to a component.
 *
 * TODO Dan: Investigate locking the component so that dependencies cannot stop while used through a weak dependency.
 * Not sure how useful it would be, since most references to components are in classes that are not registered
 * components themselves.
 *
 * @author Dan Berindei
 * @since 9.4
 */
public interface ComponentRef<T> {

   /**
    * @return the running component instance
    * @throws org.infinispan.IllegalLifecycleStateException if the component is not running
    */
   T running();

   /**
    * @return the wired component instance, which may or may not be running.
    */
   T wired();

   /**
    * @return {@code true} if all of the component's start methods have run and none of its stop methods started running
    */
   boolean isRunning();

   boolean isWired();

   String getName();
}
