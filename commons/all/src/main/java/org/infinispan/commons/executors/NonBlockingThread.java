package org.infinispan.commons.executors;

/**
 * Marker interface used to signify that the given thread is non blocking and operations that block should not
 * be run on it.
 */
public interface NonBlockingThread {
}
