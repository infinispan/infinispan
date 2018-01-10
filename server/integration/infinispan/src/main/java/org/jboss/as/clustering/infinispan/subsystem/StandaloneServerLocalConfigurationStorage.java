package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.controller.client.helpers.ClientConstants.ADD;
import static org.jboss.as.controller.client.helpers.ClientConstants.NAME;
import static org.jboss.as.controller.client.helpers.ClientConstants.OP;
import static org.jboss.as.controller.client.helpers.ClientConstants.OP_ADDR;
import static org.jboss.as.controller.client.helpers.ClientConstants.OUTCOME;
import static org.jboss.as.controller.client.helpers.ClientConstants.READ_ATTRIBUTE_OPERATION;
import static org.jboss.as.controller.client.helpers.ClientConstants.REMOVE_OPERATION;
import static org.jboss.as.controller.client.helpers.ClientConstants.SUCCESS;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.globalstate.LocalConfigurationStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.as.clustering.infinispan.InfinispanMessages;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.client.ModelControllerClient;
import org.jboss.dmr.ModelNode;

/**
 * A {@link LocalConfigurationStorage} which saves {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag#PERMANENT}
 * changes to the server configuration model in standalone mode.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public class StandaloneServerLocalConfigurationStorage implements ServerLocalConfigurationStorage {
   private static final String[] CACHE_MODES = {"local-cache", "invalidation-cache", "replicated-cache", "distributed-cache", "scattered-cache"};
   private ModelControllerClient modelControllerClient;
   private PathAddress rootPath;

   @Override
   public void initialize(EmbeddedCacheManager embeddedCacheManager) {
      // NO-OP
   }

   @Override
   public void validateFlags(EnumSet<CacheContainerAdmin.AdminFlag> flags) {
   }

   @Override
   public void createCache(String name, String template, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      if (!flags.contains(CacheContainerAdmin.AdminFlag.PERMANENT)) {
         throw InfinispanMessages.MESSAGES.cannotCreateNonPermamentCache(name);
      }

      if (template == null) {
         throw InfinispanMessages.MESSAGES.nonExistingTemplate(name, template);
      }

      String cacheMode = findCacheMode(template, true);

      if (cacheMode == null) {
         throw InfinispanMessages.MESSAGES.nonExistingTemplate(name, template);
      }

      final ModelNode cacheAddOp = new ModelNode();
      cacheAddOp.get(OP).set(ADD);
      cacheAddOp.get(OP_ADDR).set(rootPath.append(cacheMode, name).toModelNode());
      cacheAddOp.get("configuration").set(template);

      try {
         ModelNode resp = modelControllerClient.execute(cacheAddOp);
         if (!SUCCESS.equals(resp.get(OUTCOME).asString())) {
            throw InfinispanMessages.MESSAGES.cannotCreateCache(null, name);
         }
      } catch (IOException e) {
         throw InfinispanMessages.MESSAGES.cannotCreateCache(e, name);
      }
   }

   @Override
   public void removeCache(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      String cacheMode = findCacheMode(name, false);

      if (cacheMode == null) {
         throw InfinispanMessages.MESSAGES.nonExistingCache(name);
      }

      final ModelNode cacheRemoveOp = new ModelNode();
      cacheRemoveOp.get(OP).set(REMOVE_OPERATION);
      cacheRemoveOp.get(OP_ADDR).set(rootPath.append(cacheMode, name).toModelNode());

      try {
         ModelNode resp = modelControllerClient.execute(cacheRemoveOp);
         if (!SUCCESS.equals(resp.get(OUTCOME).asString())) {
            throw InfinispanMessages.MESSAGES.cannotRemoveCache(null, name);
         }
      } catch (IOException e) {
         throw InfinispanMessages.MESSAGES.cannotRemoveCache(e, name);
      }
   }

   private String findCacheMode(String name, boolean configuration) {
      for (String cacheMode : CACHE_MODES) {
         PathAddress address = rootPath;
         if (configuration) {
            address = address.append(CacheContainerConfigurationsResource.PATH).append(cacheMode + "-configuration", name);
         } else {
            address = address.append(cacheMode, name);
         }
         ModelNode op = new ModelNode();
         op.get(OP).set(READ_ATTRIBUTE_OPERATION);
         op.get(OP_ADDR).set(address.toModelNode());
         op.get(NAME).set(configuration ? "template" : "configuration");
         try {
            ModelNode resp = modelControllerClient.execute(op);
            if (SUCCESS.equals(resp.get(OUTCOME).asString())) {
               return cacheMode;
            }
         } catch (IOException e) {
            // Ignore and try the next
         }
      }
      // Nothing found
      return null;
   }

   @Override
   public Map<String, Configuration> loadAll() {
      // No need, persistent configurations will already have been loaded
      return Collections.emptyMap();
   }

   @Override
   public void setRootPath(PathAddress rootPath) {
      this.rootPath = rootPath;
   }

   @Override
   public void setModelControllerClient(ModelControllerClient modelControllerClient) {
      this.modelControllerClient = modelControllerClient;
   }
}
