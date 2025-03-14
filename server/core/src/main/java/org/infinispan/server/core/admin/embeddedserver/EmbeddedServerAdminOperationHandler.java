package org.infinispan.server.core.admin.embeddedserver;

import org.infinispan.server.core.admin.AdminOperationsHandler;

/**
 * EmbeddedServerAdminOperationHandler is an implementation of {@link AdminOperationsHandler} which uses
 * {@link org.infinispan.commons.api.CacheContainerAdmin} to apply changes to the cache manager configuration
 *
 * @since 9.1
 */
public class EmbeddedServerAdminOperationHandler extends AdminOperationsHandler {

   public EmbeddedServerAdminOperationHandler() {
      super(
            new CacheAssignAliasTask(),
            new CacheCreateTask(),
            new CacheGetOrCreateTask(),
            new CacheNamesTask(),
            new CacheRemoveTask(),
            new CacheReindexTask(),
            new CacheUpdateIndexSchemaTask(),
            new CacheUpdateConfigurationAttributeTask(),
            new TemplateCreateTask(),
            new TemplateRemoveTask(),
            new TemplateNamesTask()
      );
   }
}
