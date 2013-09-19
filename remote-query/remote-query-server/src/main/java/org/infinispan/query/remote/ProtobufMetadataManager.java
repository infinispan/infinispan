package org.infinispan.query.remote;

import com.google.protobuf.Descriptors;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.transaction.TransactionMode;

import javax.management.ObjectName;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

//todo [anistor] use ClusterRegistry instead of reinventing it
/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@Scope(Scopes.GLOBAL)
@MBean(objectName = ProtobufMetadataManager.OBJECT_NAME,
       description = "Component that acts as a manager and container for Protocol Buffers metadata descriptors in the scope of a CacheManger.")
public class ProtobufMetadataManager {

   public static final String OBJECT_NAME = "ProtobufMetadataManager";

   private static final String METADATA_CACHE_NAME = "__ProtobufMetadataManager__";

   private ObjectName objectName;

   private final EmbeddedCacheManager cacheManager;

   private Cache<String, byte[]> metadataCache;

   private final SerializationContext serCtx = ProtobufUtil.newSerializationContext();

   public ProtobufMetadataManager(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   public ObjectName getObjectName() {
      return objectName;
   }

   public void setObjectName(ObjectName objectName) {
      this.objectName = objectName;
   }

   private Cache<String, byte[]> getMetadataCache() {
      if (metadataCache == null) {
         synchronized (this) {
            if (metadataCache == null) {
               cacheManager.defineConfiguration(METADATA_CACHE_NAME, getMetadataCacheConfig());
               metadataCache = cacheManager.getCache(METADATA_CACHE_NAME);
               metadataCache.addListener(new MetadataCacheListener());
            }
         }
      }
      return metadataCache;
   }

   private Configuration getMetadataCacheConfig() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

      //allow the registry to work for local caches as well as isClustered caches
      boolean isClustered = cacheManager.getGlobalComponentRegistry().getGlobalConfiguration().isClustered();
      configurationBuilder.clustering().cacheMode(isClustered ? CacheMode.REPL_SYNC : CacheMode.LOCAL);

      //use a transactional cache for high consistency as writes are expected to be rare in this cache
      configurationBuilder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);

      //fetch the state (redundant as state transfer this is enabled by default, keep it here to document the intention)
      configurationBuilder.clustering().stateTransfer().fetchInMemoryState(true);

      return configurationBuilder.build();
   }

   public <T> void registerMarshaller(Class<? extends T> clazz, BaseMarshaller<T> marshaller) {
      serCtx.registerMarshaller(clazz, marshaller);
   }

   @ManagedOperation(description = "Registers a Protobuf definition file", displayName = "Register Protofile")
   public void registerProtofile(byte[] descriptorFile) throws IOException, Descriptors.DescriptorValidationException {
      getMetadataCache().put(UUID.randomUUID().toString(), descriptorFile);
   }

   public void registerProtofile(InputStream descriptorFile) throws IOException, Descriptors.DescriptorValidationException {
      registerProtofile(readStream(descriptorFile));
   }

   public void registerProtofile(String classpathResource) throws IOException, Descriptors.DescriptorValidationException {
      InputStream is = getClass().getResourceAsStream(classpathResource);
      if (is == null) {
         throw new IllegalArgumentException("Missing resource: " + classpathResource);
      }
      registerProtofile(is);
   }

   private byte[] readStream(InputStream is) throws IOException {
      try {
         ByteArrayOutputStream os = new ByteArrayOutputStream();
         byte[] buf = new byte[1024];
         int len;
         while ((len = is.read(buf)) != -1) {
            os.write(buf, 0, len);
         }
         return os.toByteArray();
      } finally {
         is.close();
      }
   }

   SerializationContext getSerializationContext() {
      return serCtx;
   }

   public static SerializationContext getSerializationContext(EmbeddedCacheManager cacheManager) {
      if (cacheManager == null) {
         throw new IllegalArgumentException("cacheManager cannot be null");
      }
      ProtobufMetadataManager metadataManager = cacheManager.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);
      if (metadataManager == null) {
         throw new IllegalStateException("ProtobufMetadataManager not initialised yet!");
      }
      return metadataManager.getSerializationContext();
   }

   @Listener
   public class MetadataCacheListener {

      @CacheEntryCreated
      public void created(CacheEntryCreatedEvent<String, byte[]> e) throws IOException, Descriptors.DescriptorValidationException {
         if (!e.isPre()) {
            registerProtofile(e.getValue());
         }
      }

      @CacheEntryModified
      public void modified(CacheEntryModifiedEvent<String, byte[]> e) throws IOException, Descriptors.DescriptorValidationException {
         if (!e.isPre()) {
            registerProtofile(e.getValue());
         }
      }

      private void registerProtofile(byte[] descriptorFile) throws IOException, Descriptors.DescriptorValidationException {
         getSerializationContext().registerProtofile(new ByteArrayInputStream(descriptorFile));
      }
   }
}
