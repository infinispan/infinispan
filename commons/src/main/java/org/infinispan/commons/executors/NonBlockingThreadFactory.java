package org.infinispan.commons.executors;

import java.util.concurrent.ThreadFactory;

/**
 * Same as {@link ThreadFactory} except that threads created by this factory should be non blocking and implement the
 * {@link NonBlockingThread} interface.
 */
public interface NonBlockingThreadFactory extends ThreadFactory {
}
