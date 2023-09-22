package org.infinispan.commons.spi;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * @since 15.0
 **/
public interface ThreadCreator {
   Thread createThread(ThreadGroup threadGroup, Runnable target, boolean lightweight);

   Optional<ExecutorService> newVirtualThreadPerTaskExecutor();
}
