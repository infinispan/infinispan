package org.infinispan.executors;

import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Used to configure and create scheduled executors
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface ScheduledExecutorFactory {
   ScheduledExecutorService getScheduledExecutor(Properties p);
}
