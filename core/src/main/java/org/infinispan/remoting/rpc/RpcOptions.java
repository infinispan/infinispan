package org.infinispan.remoting.rpc;

import java.util.concurrent.TimeUnit;

import org.infinispan.remoting.inboundhandler.DeliverOrder;

/**
 * Classes that wraps all the configuration parameters to configure a remote invocation.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class RpcOptions {

   private final long timeout;
   private final TimeUnit unit;
   private final DeliverOrder deliverOrder;

   /**
    * @since 9.2
    */
   public RpcOptions(DeliverOrder deliverOrder, long timeout, TimeUnit unit) {
      if (unit == null) {
         throw new IllegalArgumentException("TimeUnit cannot be null");
      } else if (deliverOrder == null) {
         throw new IllegalArgumentException("DeliverMode cannot be null");
      }
      this.timeout = timeout;
      this.unit = unit;
      this.deliverOrder = deliverOrder;
   }

   /**
    * @return the timeout value to give up.
    */
   public long timeout() {
      return timeout;
   }

   /**
    * @return the {@link TimeUnit} in which the timeout value is.
    */
   public TimeUnit timeUnit() {
      return unit;
   }

   /**
    * @return the {@link org.infinispan.remoting.inboundhandler.DeliverOrder}.
    */
   public DeliverOrder deliverOrder() {
      return deliverOrder;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RpcOptions options = (RpcOptions) o;

      return timeout == options.timeout &&
            deliverOrder == options.deliverOrder &&
            unit == options.unit;
   }

   @Override
   public int hashCode() {
      int result = (int) (timeout ^ (timeout >>> 32));
      result = 31 * result + unit.hashCode();
      result = 31 * result + deliverOrder.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "RpcOptions{" +
            "timeout=" + timeout +
            ", unit=" + unit +
            ", deliverOrder=" + deliverOrder +
            '}';
   }
}
