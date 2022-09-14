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
    * The message is delivered as soon as it is received and, it is not affected by any flow control algorithm. No order
    * is ensured between messages.
    */
   NONE_NO_FC {
      @Override
      public boolean preserveOrder() {
         return false;
      }

      @Override
      public boolean skipFlowControl() {
         return true;
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
    * The message is delivered by the order they are sent and, it is not affected by any flow control algorithm. If a
    * node sends M1 and M2 to other node, it delivers first M1 and then M2.
    */
   PER_SENDER_NO_FC {
      @Override
      public boolean preserveOrder() {
         return true;
      }

      @Override
      public boolean skipFlowControl() {
         return true;
      }
   };

   public abstract boolean preserveOrder();

   public boolean skipFlowControl() {
      return false;
   }
}
