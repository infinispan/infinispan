package org.infinispan.executors;

import java.security.AccessControlContext;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

public interface SecurityAwareScheduledExecutorFactory extends ScheduledExecutorFactory {
   ScheduledExecutorService getScheduledExecutor(Properties p, AccessControlContext context);
}
