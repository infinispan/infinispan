package org.infinispan.executors;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MBean
@Scope(Scopes.GLOBAL)
public abstract class ManageableExecutorService<T extends ExecutorService> {

   // volatile so reads don't have to be in a synchronized block
   protected volatile T executor;

   @ManagedAttribute(
         description = "Returns the number of threads in this executor.",
         displayName = "Number of executor threads",
         dataType = DataType.TRAIT,
         displayType = DisplayType.SUMMARY
   )
   public int getPoolSize() {
      if (executor instanceof ThreadPoolExecutor) {
         return ((ThreadPoolExecutor) executor).getPoolSize();
      } else {
         return -1;
      }
   }

   @ManagedAttribute(
         description = "Returns the number of active executor threads.",
         displayName = "Number of active executor threads",
         dataType = DataType.TRAIT,
         displayType = DisplayType.SUMMARY
   )
   public int getActiveCount() {
      if (executor instanceof ThreadPoolExecutor) {
         return ((ThreadPoolExecutor) executor).getActiveCount();
      } else {
         return -1;
      }
   }

   @ManagedAttribute(
         description = "Returns the maximum number of executor threads.",
         displayName = "Maximum number of executor threads",
         dataType = DataType.TRAIT,
         displayType = DisplayType.SUMMARY,
         writable = true
   )
   public int getMaximumPoolSize() {
      if (executor instanceof ThreadPoolExecutor) {
         return ((ThreadPoolExecutor) executor).getMaximumPoolSize();
      } else {
         return -1;
      }
   }

   public void setMaximumPoolSize(int maximumPoolSize) {
      if (executor instanceof ThreadPoolExecutor) {
         ((ThreadPoolExecutor) executor).setMaximumPoolSize(maximumPoolSize);
         if (!(((ThreadPoolExecutor)executor).getQueue() instanceof SynchronousQueue)) {
            ((ThreadPoolExecutor) executor).setCorePoolSize(maximumPoolSize);
         }
      } else {
         throw new UnsupportedOperationException();
      }
   }

   @ManagedAttribute(
         description = "Returns the largest ever number of executor threads.",
         displayName = "Largest number of executor threads",
         dataType = DataType.TRAIT,
         displayType = DisplayType.SUMMARY
   )
   public int getLargestPoolSize() {
      if (executor instanceof ThreadPoolExecutor) {
         return ((ThreadPoolExecutor) executor).getLargestPoolSize();
      } else {
         return -1;
      }
   }

   @ManagedAttribute(
         description = "Returns the number of elements in this executor's queue.",
         displayName = "Elements in the queue",
         dataType = DataType.TRAIT,
         displayType = DisplayType.SUMMARY
   )
   public int getQueueSize() {
      if (executor instanceof ThreadPoolExecutor) {
         return ((ThreadPoolExecutor) executor).getQueue().size();
      } else {
         return -1;
      }
   }

   @ManagedAttribute(
         description = "Returns the keep-alive time for this pool's threads",
         displayName = "Keep-alive for pooled threads",
         dataType = DataType.TRAIT,
         displayType = DisplayType.SUMMARY
   )
   public long getKeepAliveTime() {
      if (executor instanceof ThreadPoolExecutor) {
         return ((ThreadPoolExecutor) executor).getKeepAliveTime(TimeUnit.MILLISECONDS);
      } else {
         return -1;
      }
   }

   public void setKeepAliveTime(long milliseconds) {
      if (executor instanceof ThreadPoolExecutor) {
         ((ThreadPoolExecutor) executor).setKeepAliveTime(milliseconds, TimeUnit.MILLISECONDS);
      } else {
         throw new UnsupportedOperationException();
      }
   }

}
