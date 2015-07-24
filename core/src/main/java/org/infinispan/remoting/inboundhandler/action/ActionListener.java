package org.infinispan.remoting.inboundhandler.action;

/**
 * A listener that is invoked when an {@link Action} is completed.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface ActionListener {

   /**
    * Invoked when an {@link Action} is completed.
    */
   void onComplete();

}
