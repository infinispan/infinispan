package org.infinispan.commons.spi;

/**
 * @since 15.0
 **/
public interface ThreadCreator {
   Thread createThread(ThreadGroup threadGroup, Runnable target, boolean lightweight);
}
