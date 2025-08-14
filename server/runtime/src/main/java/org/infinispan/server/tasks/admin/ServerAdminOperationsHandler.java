package org.infinispan.server.tasks.admin;

import org.infinispan.server.core.admin.AdminOperationsHandler;
import org.infinispan.server.core.admin.AdminServerTask;
import org.infinispan.server.core.admin.embeddedserver.CacheAssignAliasTask;
import org.infinispan.server.core.admin.embeddedserver.CacheNamesTask;
import org.infinispan.server.core.admin.embeddedserver.CacheReindexTask;
import org.infinispan.server.core.admin.embeddedserver.CacheRemoveTask;
import org.infinispan.server.core.admin.embeddedserver.CacheUpdateConfigurationAttributeTask;
import org.infinispan.server.core.admin.embeddedserver.CacheUpdateIndexSchemaTask;
import org.infinispan.server.core.admin.embeddedserver.TemplateCreateTask;
import org.infinispan.server.core.admin.embeddedserver.TemplateRemoveTask;
import org.infinispan.server.core.admin.embeddedserver.ValidateSafeSchemaDeleteTask;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ServerAdminOperationsHandler extends AdminOperationsHandler {

   public ServerAdminOperationsHandler() {
      super(generateTasks());
   }

   // This method is referenced by quarkus, if method declaration is changed or moved it must be updated in Quarkus
   // Infinispan as well
   private static AdminServerTask<?>[] generateTasks() {
      String includeLoggingResource = System.getProperty("infinispan.server.resource.logging", "true");
      if (Boolean.parseBoolean(includeLoggingResource)) {
         return new AdminServerTask[]{
               new CacheAssignAliasTask(),
               new CacheCreateTask(),
               new CacheGetOrCreateTask(),
               new CacheNamesTask(),
               new CacheRemoveTask(),
               new CacheReindexTask(),
               new CacheUpdateConfigurationAttributeTask(),
               new CacheUpdateIndexSchemaTask(),
               new LoggingSetTask(),
               new LoggingRemoveTask(),
               new TemplateCreateTask(),
               new TemplateRemoveTask(),
               new ValidateSafeSchemaDeleteTask()
         };
      } else {
         return generateTasksWithoutLogging();
      }
   }

   // This method is referenced by quarkus, if method declaration is changed or moved it must be updated in Quarkus
   // Infinispan as well
   private static AdminServerTask<?>[] generateTasksWithoutLogging() {
      return new AdminServerTask[]{
            new CacheAssignAliasTask(),
            new CacheCreateTask(),
            new CacheGetOrCreateTask(),
            new CacheNamesTask(),
            new CacheRemoveTask(),
            new CacheReindexTask(),
            new CacheUpdateConfigurationAttributeTask(),
            new CacheUpdateIndexSchemaTask(),
            new TemplateCreateTask(),
            new TemplateRemoveTask(),
            new ValidateSafeSchemaDeleteTask()
      };
   }
}
