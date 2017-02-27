package org.infinispan.remoting.inboundhandler;

/**
 * Interface responsible to send back the return value to the sender.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public interface Reply {

   Reply NO_OP = returnValue -> {};

   /**
    * Sends back the return value to the sender.
    *
    * @param returnValue the return value
    */
   void reply(Object returnValue);
}
