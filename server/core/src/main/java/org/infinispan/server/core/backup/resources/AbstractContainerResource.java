package org.infinispan.server.core.backup.resources;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.server.core.BackupManager;
import org.infinispan.server.core.backup.ContainerResource;
import org.infinispan.server.core.logging.Log;
import org.infinispan.util.concurrent.BlockingManager;

abstract class AbstractContainerResource implements ContainerResource {

   protected static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   protected final BackupManager.Resources.Type type;
   protected final BackupManager.Resources params;
   protected final Path root;
   protected final BlockingManager blockingManager;
   protected final boolean wildcard;
   protected final Set<String> resources;

   protected AbstractContainerResource(BackupManager.Resources.Type type, BackupManager.Resources params,
                                       BlockingManager blockingManager, Path root) {
      this.type = type;
      this.params = params;
      this.root = root.resolve(type.toString());
      this.blockingManager = blockingManager;
      Set<String> qualifiedResources = params.getQualifiedResources(type);
      this.wildcard = qualifiedResources == null;
      this.resources = ConcurrentHashMap.newKeySet();
      if (!wildcard)
         this.resources.addAll(qualifiedResources);
   }

   @Override
   public void writeToManifest(Properties properties) {
      properties.put(type.toString(), String.join(",", resources));
   }

   @Override
   public void prepareAndValidateRestore(Properties properties) {
      // Only process specific resources if specified
      Set<String> resourcesAvailable = asSet(properties, type);
      if (wildcard) {
         resources.addAll(resourcesAvailable);
      } else {
         resourcesAvailable.retainAll(resources);

         if (resourcesAvailable.isEmpty()) {
            Set<String> missingResources = new HashSet<>(resources);
            missingResources.removeAll(resourcesAvailable);
            throw log.unableToFindBackupResource(type.toString(), missingResources);
         }
      }
   }

   static Set<String> asSet(Properties properties, BackupManager.Resources.Type resource) {
      String prop = properties.getProperty(resource.toString());
      if (prop == null || prop.isEmpty())
         return Collections.emptySet();
      String[] resources = prop.split(",");
      Set<String> set = new HashSet<>(resources.length);
      Collections.addAll(set, resources);
      return set;
   }

   protected void mkdirs(Path path) {
      try {
         Files.createDirectories(path);
      } catch (IOException e) {
         throw new CacheException("Unable to create directories " + path);
      }
   }

   protected static void writeMessageStream(Object o, ImmutableSerializationContext serCtx, DataOutputStream output) throws IOException {
      // It's necessary to first write the length of each message stream as the Protocol Buffer wire format is not self-delimiting
      // https://developers.google.com/protocol-buffers/docs/techniques#streaming
      byte[] b = ProtobufUtil.toByteArray(serCtx, o);
      output.writeInt(b.length);
      output.write(b);
      output.flush();
   }

   protected static <T> T readMessageStream(ImmutableSerializationContext ctx, Class<T> clazz, DataInputStream is) throws IOException {
      int length = is.readInt();
      byte[] b = new byte[length];
      is.readFully(b);
      return ProtobufUtil.fromByteArray(ctx, b, clazz);
   }

   static boolean isInternalName(String name) {
      return name !=null && (name.startsWith("org.infinispan") || name.startsWith("example.") || "memcachedCache".equals(name) || "respCache".equals(name));
   }
}
