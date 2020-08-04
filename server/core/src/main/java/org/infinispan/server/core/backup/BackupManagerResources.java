package org.infinispan.server.core.backup;

import static org.infinispan.server.core.BackupManager.Resources.Type.CACHES;
import static org.infinispan.server.core.BackupManager.Resources.Type.CACHE_CONFIGURATIONS;
import static org.infinispan.server.core.BackupManager.Resources.Type.COUNTERS;
import static org.infinispan.server.core.BackupManager.Resources.Type.PROTO_SCHEMAS;
import static org.infinispan.server.core.BackupManager.Resources.Type.SCRIPTS;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.server.core.BackupManager;

/**
 * @author Ryan Emerson
 * @since 12.0
 */
public class BackupManagerResources implements BackupManager.Resources {

   final Map<Type, Set<String>> resources;

   public BackupManagerResources(Map<Type, Set<String>> resources) {
      this.resources = resources;
   }

   @Override
   public Set<Type> includeTypes() {
      return resources.keySet();
   }

   @Override
   public Set<String> getQualifiedResources(Type type) {
      Set<String> qualified = resources.get(type);
      return qualified.isEmpty() ? null : qualified;
   }

   public static class Builder {
      final Map<Type, Set<String>> resources = new HashMap<>();

      public Builder includeAll() {
         return includeAll(BackupManager.Resources.Type.values());
      }

      public Builder includeAll(Type... resources) {
         for (Type resource : resources)
            addResources(resource);
         return this;
      }

      public Builder ignore(Type... resources) {
         for (Type resource : resources)
            this.resources.remove(resource);
         return this;
      }

      public Builder addCaches(String... caches) {
         return addResources(CACHES, caches);
      }

      public Builder addCacheConfigurations(String... configs) {
         return addResources(CACHE_CONFIGURATIONS, configs);
      }

      public Builder addCounters(String... counters) {
         return addResources(COUNTERS, counters);
      }

      public Builder addProtoSchemas(String... schemas) {
         return addResources(PROTO_SCHEMAS, schemas);
      }

      public Builder addScripts(String... scripts) {
         return addResources(SCRIPTS, scripts);
      }

      public Builder addResources(Type resource, String... resources) {
         return addResources(resource, Arrays.asList(resources));
      }

      public Builder addResources(Type resource, Collection<String> resources) {
         this.resources.compute(resource, (k, v) -> {
            Set<String> set = v == null ? new HashSet<>() : v;
            set.addAll(resources);
            return set;
         });
         return this;
      }

      public BackupManagerResources build() {
         return new BackupManagerResources(resources);
      }
   }
}
