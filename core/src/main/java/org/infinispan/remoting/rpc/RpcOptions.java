/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.remoting.rpc;

import java.util.concurrent.TimeUnit;

/**
 * Classes that wraps all the configuration parameters to configure a remote invocation.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class RpcOptions {

   private final long timeout;
   private final TimeUnit unit;
   private final boolean fifoOrder;
   private final boolean totalOrder;
   private final ResponseFilter responseFilter;
   private final ResponseMode responseMode;
   private final boolean skipReplicationQueue;

   public RpcOptions(long timeout, TimeUnit unit, ResponseFilter responseFilter,
                     ResponseMode responseMode, boolean skipReplicationQueue, boolean fifoOrder, boolean totalOrder) {
      if (unit == null) {
         throw new IllegalArgumentException("TimeUnit cannot be null");
      } else if (responseMode == null) {
         throw new IllegalArgumentException("ResponseMode cannot be null");
      }
      this.timeout = timeout;
      this.unit = unit;
      this.fifoOrder = fifoOrder;
      this.totalOrder = totalOrder;
      this.responseFilter = responseFilter;
      this.responseMode = responseMode;
      this.skipReplicationQueue = skipReplicationQueue;
   }

   /**
    * @return the timeout value to give up.
    */
   public long timeout() {
      return timeout;
   }

   /**
    * @return  the {@link TimeUnit} in which the timeout value is.
    */
   public TimeUnit timeUnit() {
      return unit;
   }

   /**
    * @return  {@code true} if the message is to be delivered in FIFO order.
    */
   public boolean fifoOrder() {
      return fifoOrder;
   }

   /**
    * @return  {@code true} if the message is to be delivered in total order.
    */
   public boolean totalOrder() {
      return totalOrder;
   }

   /**
    * @return the {@link ResponseFilter} to be used. Default is {@code null} meaning waiting for all or none responses
    *         depending if the remote invocation is synchronous or asynchronous respectively.
    */
   public ResponseFilter responseFilter() {
      return responseFilter;
   }

   /**
    * @return the {@link ResponseMode} to handle with the responses.
    */
   public ResponseMode responseMode() {
      return responseMode;
   }

   /**
    * @return if {@code true}, the remote invocation will never be dispatch to the {@link
    *         org.infinispan.remoting.ReplicationQueue}. However, only asynchronous remote invocation may be dispatched
    *         to the {@link org.infinispan.remoting.ReplicationQueue}.
    */
   public boolean skipReplicationQueue() {
      return skipReplicationQueue;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RpcOptions options = (RpcOptions) o;

      if (skipReplicationQueue != options.skipReplicationQueue) return false;
      if (timeout != options.timeout) return false;
      if (fifoOrder != options.fifoOrder) return false;
      if (totalOrder != options.totalOrder) return false;
      if (responseFilter != null ? !responseFilter.equals(options.responseFilter) : options.responseFilter != null)
         return false;
      if (responseMode != options.responseMode) return false;
      if (unit != options.unit) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (int) (timeout ^ (timeout >>> 32));
      result = 31 * result + unit.hashCode();
      result = 31 * result + (fifoOrder ? 1 : 0);
      result = 31 * result + (totalOrder ? 1 : 0);
      result = 31 * result + (responseFilter != null ? responseFilter.hashCode() : 0);
      result = 31 * result + responseMode.hashCode();
      result = 31 * result + (skipReplicationQueue ? 1 : 0);
      return result;
   }

   @Override
   public String toString() {
      return "RpcOptions{" +
            "timeout=" + timeout +
            ", unit=" + unit +
            ", fifoOrder=" + fifoOrder +
            ", totalOrder=" + totalOrder +
            ", responseFilter=" + responseFilter +
            ", responseMode=" + responseMode +
            ", skipReplicationQueue=" + skipReplicationQueue +
            '}';
   }
}
