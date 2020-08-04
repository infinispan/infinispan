package org.infinispan.server.core.backup.resources;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.BackupManager;
import org.infinispan.server.core.backup.ContainerResource;
import org.infinispan.server.core.logging.Log;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * Factory for creating the {@link ContainerResource}s required for a backup/restore operation.
 *
 * @author Ryan Emerson
 * @since 12.0
 */
public class ContainerResourceFactory {

   private static final Log log = LogFactory.getLog(ContainerResourceFactory.class, Log.class);

   public static Collection<ContainerResource> getResources(BackupManager.Resources params, BlockingManager blockingManager,
                                                            EmbeddedCacheManager cm, ParserRegistry parserRegistry,
                                                            Path containerRoot) {
      GlobalComponentRegistry gcr = cm.getGlobalComponentRegistry();
      return params.includeTypes().stream()
            .map(type -> {
               switch (type) {
                  case CACHES:
                     return new CacheResource(blockingManager, parserRegistry, cm, params, containerRoot);
                  case CACHE_CONFIGURATIONS:
                     return new CacheConfigResource(blockingManager, parserRegistry, cm, params, containerRoot);
                  case COUNTERS:
                     CounterManager counterManager = gcr.getComponent(CounterManager.class);
                     return counterManager == null ?
                           missingResource(type) :
                           new CounterResource(blockingManager, cm, params, containerRoot);
                  case PROTO_SCHEMAS:
                  case SCRIPTS:
                     ContainerResource cr = InternalCacheResource.create(type, blockingManager, cm, params, containerRoot);
                     return cr == null ? missingResource(type) : cr;
                  default:
                     throw new IllegalStateException();
               }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
   }

   private static ContainerResource missingResource(BackupManager.Resources.Type type) {
      log.debugf("Unable to process resource '%s' as the required modules are not on the server's classpath'", type);
      return null;
   }
}
