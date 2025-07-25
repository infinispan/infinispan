
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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ValidateSafeSchemaDeleteTask extends AdminServerTask<byte[]> {
   private static final Set<String> PARAMETERS = Set.of("schema");
   private static final byte[] TRUE = "true".getBytes(StandardCharsets.UTF_8);
   private static final byte[] FALSE = "false".getBytes(StandardCharsets.UTF_8);

   @Override
   public String getTaskContextName() {
      return "schemas";
   }

   @Override
   public String getTaskOperationName() {
      return "validate";
   }

   @Override
   public Set<String> getParameters() {
      return PARAMETERS;
   }

   @Override
   protected byte[] execute(EmbeddedCacheManager cacheManager, Map<String, List<String>> parameters, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      String schemaName = requireParameter(parameters, "schema");
      Cache<String, String> cache = cacheManager.getCache(InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME);
      String schema = cache.get(schemaName);
      if (schema == null) {
         return TRUE;
      }

      FileDescriptorSource fileDescriptorSource = new FileDescriptorSource().addProtoFile(schemaName, schema);
      ProtostreamProtoParser parser = new ProtostreamProtoParser(Configuration.builder().build());
      FileDescriptor fileDescriptor = parser.parse(fileDescriptorSource).get(schemaName);
      List<Descriptor> types = fileDescriptor.getMessageTypes();
      for (Descriptor descriptor : types) {
         String typeName = descriptor.getFullName();
         for (String configName : cacheManager.getCacheConfigurationNames()) {
            org.infinispan.configuration.cache.Configuration cacheConfiguration = cacheManager.getCacheConfiguration(configName);
            if (cacheConfiguration.indexing().enabled() && cacheConfiguration.indexing().indexedEntityTypes().contains(typeName)) {
               return FALSE;
            }
         }
      }
      return TRUE;
   }
}
