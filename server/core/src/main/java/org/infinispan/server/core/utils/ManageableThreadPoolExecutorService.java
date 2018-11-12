package org.infinispan.server.core.utils;

import java.util.concurrent.ThreadPoolExecutor;

import org.infinispan.executors.ManageableExecutorService;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ManageableThreadPoolExecutorService extends ManageableExecutorService<ThreadPoolExecutor> {

   public ManageableThreadPoolExecutorService(ThreadPoolExecutor threadPoolExecutor) {
      this.executor = threadPoolExecutor;
   }
}
