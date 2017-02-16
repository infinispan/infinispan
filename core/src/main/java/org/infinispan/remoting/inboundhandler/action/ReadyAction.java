package org.infinispan.remoting.inboundhandler.action;

/**
 * An interface that allows the {@link org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler} to check
 * when this action is ready.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface ReadyAction {

   /**
    * @return {@code true} if ready.
    */
   boolean isReady();

   /**
    * It adds a listener that is invoked when this action is ready.
    *
    * @param listener the listener to invoke.
    */
   void addListener(ActionListener listener);

   /**
    * Cleanup when the command throws an exception while executing.
    */
   void onException();

   /**
    * Invoked always after the command is executed and the reply is sent.
    */
   void onFinally();
}
