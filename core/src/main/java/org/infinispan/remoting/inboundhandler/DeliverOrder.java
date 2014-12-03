package org.infinispan.remoting.inboundhandler;

/**
 * Used in RPC, it defines how the messages are delivered to the nodes.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public enum DeliverOrder {
   /**
    * The message is delivered as soon as it is received. No order is ensured between messages.
    */
   NONE {
      @Override
      public boolean preserveOrder() {
         return false;
      }
   },
   /**
    * The message is delivered by the order they are sent. If a node sends M1 and M2 to other node, it delivers first M1
    * and then M2.
    */
   PER_SENDER {
      @Override
      public boolean preserveOrder() {
         return true;
      }
   },
   /**
    * The message is delivered in the same order in all the destinations. If N1 sends M1 and N2 sends M2 to nodes N3 and
    * N4, if N3 delivers first M1 and then M2, then N4 also delivers first M1 and then M2.
    */
   TOTAL {
      @Override
      public boolean preserveOrder() {
         return true;
      }
   };

   public abstract boolean preserveOrder();
}
