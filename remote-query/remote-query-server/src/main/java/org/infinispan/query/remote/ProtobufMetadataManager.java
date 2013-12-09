package org.infinispan.query.remote;

import com.google.protobuf.Descriptors;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
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
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.logging.Log;
import org.infinispan.registry.ClusterRegistry;
import org.infinispan.registry.ScopedKey;
import org.infinispan.util.logging.LogFactory;

import javax.management.ObjectName;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

/**
 * A clustered repository of protobuf descriptors. All protobuf types and their marshallers must be registered with this
 * repository before being used.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@Scope(Scopes.GLOBAL)
@MBean(objectName = ProtobufMetadataManager.OBJECT_NAME,
       description = "Component that acts as a manager and container for Protocol Buffers metadata descriptors in the scope of a CacheManger.")
public class ProtobufMetadataManager implements ProtobufMetadataManagerMBean {

   private static final Log log = LogFactory.getLog(ProtobufMetadataManager.class, Log.class);

   public static final String OBJECT_NAME = "ProtobufMetadataManager";

   private static final String REGISTRY_SCOPE = ProtobufMetadataManager.class.getName();

   private ObjectName objectName;

   private ClusterRegistry<String, String, byte[]> clusterRegistry;

   private volatile ProtobufMetadataRegistryListener registryListener;

   private final SerializationContext serCtx;

   public ProtobufMetadataManager(SerializationContext serCtx) {
      this.serCtx = serCtx;
   }

   private void ensureInit() {
      if (registryListener == null) {
         synchronized (this) {
            if (registryListener == null) {
               registryListener = new ProtobufMetadataRegistryListener();
               clusterRegistry.addListener(REGISTRY_SCOPE, registryListener);

               for (String uuid : clusterRegistry.keys(REGISTRY_SCOPE)) {
                  byte[] descriptorFile = clusterRegistry.get(REGISTRY_SCOPE, uuid);
                  try {
                     serCtx.registerProtofile(new ByteArrayInputStream(descriptorFile));
                  } catch (Exception e) {
                     log.error(e);
                  }
               }
            }
         }
      }
   }

   @Inject
   protected void injectDependencies(ClusterRegistry<String, String, byte[]> clusterRegistry) {
      this.clusterRegistry = clusterRegistry;
   }

   @Stop
   protected void stop() {
      if (registryListener != null) {
         clusterRegistry.removeListener(registryListener);
         registryListener = null;
      }
   }

   public ObjectName getObjectName() {
      return objectName;
   }

   public void setObjectName(ObjectName objectName) {
      this.objectName = objectName;
   }

   public <T> void registerMarshaller(Class<? extends T> clazz, BaseMarshaller<T> marshaller) {
      ensureInit();
      serCtx.registerMarshaller(clazz, marshaller);
   }

   @ManagedOperation(description = "Registers a Protobuf definition file", displayName = "Register Protofile")
   public void registerProtofile(byte[] descriptorFile) {
      ensureInit();
      clusterRegistry.put(REGISTRY_SCOPE, UUID.randomUUID().toString(), descriptorFile);
   }

   public void registerProtofile(InputStream descriptorFile) throws IOException, Descriptors.DescriptorValidationException {
      registerProtofile(Util.readStream(descriptorFile));
   }

   public void registerProtofile(String classpathResource) throws IOException, Descriptors.DescriptorValidationException {
      InputStream is = getClass().getResourceAsStream(classpathResource);
      if (is == null) {
         throw new IllegalArgumentException("Missing resource: " + classpathResource);
      }
      registerProtofile(is);
   }

   public static SerializationContext getSerializationContext(EmbeddedCacheManager cacheManager) {
      if (cacheManager == null) {
         throw new IllegalArgumentException("cacheManager cannot be null");
      }
      ProtobufMetadataManager metadataManager = cacheManager.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);
      if (metadataManager == null) {
         throw new IllegalStateException("ProtobufMetadataManager not initialised yet!");
      }
      metadataManager.ensureInit();
      return metadataManager.serCtx;
   }

   @Listener
   class ProtobufMetadataRegistryListener {

      @CacheEntryCreated
      public void created(CacheEntryCreatedEvent<ScopedKey<String, String>, byte[]> e) throws IOException, Descriptors.DescriptorValidationException {
         if (!e.isPre()) {
            registerProtofile(e.getValue());
         }
      }

      @CacheEntryModified
      public void modified(CacheEntryModifiedEvent<ScopedKey<String, String>, byte[]> e) throws IOException, Descriptors.DescriptorValidationException {
         if (!e.isPre()) {
            registerProtofile(e.getValue());
         }
      }

      private void registerProtofile(byte[] descriptorFile) throws IOException, Descriptors.DescriptorValidationException {
         serCtx.registerProtofile(new ByteArrayInputStream(descriptorFile));
      }
   }
}
