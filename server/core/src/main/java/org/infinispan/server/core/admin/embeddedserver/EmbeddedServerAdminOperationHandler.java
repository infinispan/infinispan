package org.infinispan.server.core.admin.embeddedserver;

import org.infinispan.manager.EmbeddedCacheManagerAdmin;
import org.infinispan.server.core.admin.AdminOperationsHandler;

/**
 * EmbeddedServerAdminOperationHandler is a simple implementation of {@link AdminOperationsHandler} which uses a
 * {@link EmbeddedCacheManagerAdmin} to perform operations on all of the cluster. The approach is quite
 * fragile since new joiners will not be in sync with any caches created here.
 *
 * @since 9.1
 */
public class EmbeddedServerAdminOperationHandler extends AdminOperationsHandler {

   public EmbeddedServerAdminOperationHandler() {
      super(
            CacheCreateTask.class,
            CacheRemoveTask.class,
            CacheReindexTask.class
      );
   }

}
