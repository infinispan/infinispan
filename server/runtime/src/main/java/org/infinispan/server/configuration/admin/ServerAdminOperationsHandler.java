package org.infinispan.server.configuration.admin;

import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.server.core.admin.AdminOperationsHandler;
import org.infinispan.server.core.admin.embeddedserver.CacheNamesTask;
import org.infinispan.server.core.admin.embeddedserver.CacheReindexTask;
import org.infinispan.server.core.admin.embeddedserver.CacheRemoveTask;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ServerAdminOperationsHandler extends AdminOperationsHandler {

   public ServerAdminOperationsHandler(ConfigurationBuilderHolder defaultsHolder) {
      super(
            new CacheCreateTask(defaultsHolder),
            new CacheGetOrCreateTask(defaultsHolder),
            new CacheNamesTask(),
            new CacheRemoveTask(),
            new CacheReindexTask()
      );
   }
}
