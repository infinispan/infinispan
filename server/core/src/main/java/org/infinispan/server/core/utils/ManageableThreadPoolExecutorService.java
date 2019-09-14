package org.infinispan.server.core.utils;

import java.util.concurrent.ThreadPoolExecutor;

import org.infinispan.executors.ManageableExecutorService;
import org.infinispan.jmx.annotations.MBean;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 */
@MBean(objectName = "WorkerExecutor")
public class ManageableThreadPoolExecutorService extends ManageableExecutorService<ThreadPoolExecutor> {

   public ManageableThreadPoolExecutorService(ThreadPoolExecutor threadPoolExecutor) {
      this.executor = threadPoolExecutor;
   }
}
