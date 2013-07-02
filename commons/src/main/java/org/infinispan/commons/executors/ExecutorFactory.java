package org.infinispan.commons.executors;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

/**
 * Used to configure and create executors
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface ExecutorFactory {
   ExecutorService getExecutor(Properties p);
}
