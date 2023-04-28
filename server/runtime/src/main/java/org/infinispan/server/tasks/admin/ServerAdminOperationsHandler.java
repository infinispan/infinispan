package org.infinispan.server.tasks.admin;

import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.server.core.admin.AdminOperationsHandler;
import org.infinispan.server.core.admin.AdminServerTask;
import org.infinispan.server.core.admin.embeddedserver.CacheNamesTask;
import org.infinispan.server.core.admin.embeddedserver.CacheReindexTask;
import org.infinispan.server.core.admin.embeddedserver.CacheRemoveTask;
import org.infinispan.server.core.admin.embeddedserver.CacheUpdateConfigurationAttributeTask;
import org.infinispan.server.core.admin.embeddedserver.CacheUpdateIndexSchemaTask;
import org.infinispan.server.core.admin.embeddedserver.TemplateCreateTask;
import org.infinispan.server.core.admin.embeddedserver.TemplateRemoveTask;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ServerAdminOperationsHandler extends AdminOperationsHandler {

   public ServerAdminOperationsHandler(ConfigurationBuilderHolder defaultsHolder) {
      super(generateTasks(defaultsHolder));
   }

   // This method is referenced by quarkus, if method declaration is changed or moved it must be updated in Quarkus
   // Infinispan as well
   private static AdminServerTask<?>[] generateTasks(ConfigurationBuilderHolder defaultsHolder) {
      String includeLoggingResource = System.getProperty("infinispan.server.resource.logging", "true");
      if (Boolean.parseBoolean(includeLoggingResource)) {
         return new AdminServerTask[]{
               new CacheCreateTask(defaultsHolder),
               new CacheGetOrCreateTask(defaultsHolder),
               new CacheNamesTask(),
               new CacheRemoveTask(),
               new CacheReindexTask(),
               new CacheUpdateConfigurationAttributeTask(),
               new CacheUpdateIndexSchemaTask(),
               new LoggingSetTask(),
               new LoggingRemoveTask(),
               new TemplateCreateTask(),
               new TemplateRemoveTask()
         };
      } else {
         return generateTasksWithoutLogging(defaultsHolder);
      }
   }

   // This method is referenced by quarkus, if method declaration is changed or moved it must be updated in Quarkus
   // Infinispan as well
   private static AdminServerTask<?>[] generateTasksWithoutLogging(ConfigurationBuilderHolder defaultsHolder) {
      return new AdminServerTask[]{
            new CacheCreateTask(defaultsHolder),
            new CacheGetOrCreateTask(defaultsHolder),
            new CacheNamesTask(),
            new CacheRemoveTask(),
            new CacheReindexTask(),
            new CacheUpdateConfigurationAttributeTask(),
            new CacheUpdateIndexSchemaTask(),
            new TemplateCreateTask(),
            new TemplateRemoveTask()
      };
   }
}
