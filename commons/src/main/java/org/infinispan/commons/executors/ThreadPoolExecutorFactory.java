package org.infinispan.commons.executors;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * @author Galder Zamarreño
 */
public interface ThreadPoolExecutorFactory {

   <T extends ExecutorService> T createExecutor(ThreadFactory factory);

   /**
    * Validate parameters for the thread pool executor factory
    */
   void validate();

}
