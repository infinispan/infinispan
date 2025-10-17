
package org.infinispan.server.core.admin.embeddedserver;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.internal.InternalCacheNames;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FileDescriptor;
import org.infinispan.protostream.impl.parser.ProtostreamProtoParser;
import org.infinispan.server.core.admin.AdminServerTask;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Safely deletes a ProtoStream schema from the server.
 * <p>
 * If the schema to be removed defines an entity that is referenced by a cache,
 * the deletion is aborted to prevent breaking existing cache entries.
 *
 * @since 16.0
 */
public class SchemaSafeDeleteTask extends AdminServerTask<byte[]> {
   private static final Set<String> PARAMETERS = Set.of("name");
   private static final byte[] NOT_SAFE_DELETE = "-1".getBytes(StandardCharsets.UTF_8);
   private static final byte[] NOT_EXISTS = "0".getBytes(StandardCharsets.UTF_8);
   private static final byte[] DONE_SAFE_DELETE = "1".getBytes(StandardCharsets.UTF_8);

   @Override
   public String getTaskContextName() {
      return "schemas";
   }

   @Override
   public String getTaskOperationName() {
      return "delete";
   }

   @Override
   public Set<String> getParameters() {
      return PARAMETERS;
   }

   @Override
   protected byte[] execute(EmbeddedCacheManager cacheManager, Map<String, List<String>> parameters, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      String schemaName = requireParameter(parameters, "name");
      Cache<String, String> schemasInternalCache = cacheManager.getCache(InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME);
      String schema = schemasInternalCache.get(schemaName);

      // Schema does not exist
      if (schema == null) {
         return NOT_EXISTS;
      }

      List<Descriptor> types;
      try {
         FileDescriptorSource fileDescriptorSource = new FileDescriptorSource().addProtoFile(schemaName, schema);
         ProtostreamProtoParser parser = new ProtostreamProtoParser(Configuration.builder().build());
         FileDescriptor fileDescriptor = parser.parse(fileDescriptorSource).get(schemaName);
         types = fileDescriptor.getMessageTypes();
      } catch (Exception parseError) {
         log.error(parseError);
         // If the content can't be parsed, empty list
         types = Collections.emptyList();
      }

      for (Descriptor descriptor : types) {
         String typeName = descriptor.getFullName();
         for (String configName : cacheManager.getCacheConfigurationNames()) {
            org.infinispan.configuration.cache.Configuration cacheConfiguration = cacheManager.getCacheConfiguration(configName);
            if (cacheConfiguration.indexing().enabled() && cacheConfiguration.indexing().indexedEntityTypes().contains(typeName)) {
               return NOT_SAFE_DELETE;
            }
         }
      }

      // Remove the schema
      schemasInternalCache.remove(schemaName);

      return DONE_SAFE_DELETE;
   }
}
