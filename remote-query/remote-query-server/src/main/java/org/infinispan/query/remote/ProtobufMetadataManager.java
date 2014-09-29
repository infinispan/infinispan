package org.infinispan.query.remote;

import org.infinispan.commons.util.Util;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.logging.Log;
import org.infinispan.registry.ClusterRegistry;
import org.infinispan.registry.ScopedKey;
import org.infinispan.util.logging.LogFactory;

import javax.management.MBeanException;
import javax.management.ObjectName;
import java.io.IOException;

/**
 * A clustered repository of protobuf descriptors. All protobuf types and their marshallers must be registered with this
 * repository before being used.
 *
 * @author anistor@redhat.com
 * @author gustavonalle
 * @since 6.0
 */
@Scope(Scopes.GLOBAL)
@MBean(objectName = ProtobufMetadataManager.OBJECT_NAME,
        description = "Component that acts as a manager and container for Protocol Buffers metadata descriptors in the scope of a CacheManger.")
public class ProtobufMetadataManager implements ProtobufMetadataManagerMBean {

   private static final Log log = LogFactory.getLog(ProtobufMetadataManager.class, Log.class);

   public static final String OBJECT_NAME = "ProtobufMetadataManager";

   private static final String REGISTRY_SCOPE = ProtobufMetadataManager.class.getName();

   private static final String REGISTRY_KEY = "_descriptors";

   private ObjectName objectName;

   private ClusterRegistry<String, String, FileDescriptorSource> clusterRegistry;

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
               FileDescriptorSource descriptorSource = getFileDescriptorSource();
               if (!descriptorSource.getFileDescriptors().isEmpty()) {
                  try {
                     serCtx.registerProtoFiles(descriptorSource);
                  } catch (Exception e) {
                     log.error(e);
                  }
               }
            }
         }
      }
   }

   @Inject
   protected void injectDependencies(ClusterRegistry<String, String, FileDescriptorSource> clusterRegistry) {
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

   public void registerMarshaller(BaseMarshaller<?> marshaller) {
      ensureInit();
      serCtx.registerMarshaller(marshaller);
   }

   @ManagedOperation(description = "Registers a set of Protobuf definition files", displayName = "Register Protofiles")
   public void registerProtofiles(@Parameter(name = "fileNames", description = "names of the protofiles") String[] names,
                                  @Parameter(name = "fileContents", description = "content of the files") String[] contents)
           throws Exception {
      if (names.length != contents.length)
         throw new MBeanException(new IllegalArgumentException("invalid parameter sizes"));
      FileDescriptorSource fileDescriptorSource = getFileDescriptorSource();
      for (int i = 0; i < names.length; i++) {
         fileDescriptorSource.addProtoFile(names[i], contents[i]);
      }
      clusterRegistry.put(REGISTRY_SCOPE, REGISTRY_KEY, fileDescriptorSource);
   }

   @ManagedOperation(description = "Registers a Protobuf definition file", displayName = "Register Protofile")
   public void registerProtofile(@Parameter(name = "fileName", description = "the name of the .proto file") String name,
                                 @Parameter(name = "contents", description = "contents of the file") String contents) throws Exception {
      FileDescriptorSource fileDescriptorSource = getFileDescriptorSource();
      fileDescriptorSource.addProtoFile(name, contents);

      clusterRegistry.put(REGISTRY_SCOPE, REGISTRY_KEY, fileDescriptorSource);
   }

   @ManagedOperation(description = "Display a protobuf definition file", displayName = "Register Protofile")
   public String displayProtofile(@Parameter(name = "fileName", description = "the name of the .proto file") String name) {
      FileDescriptorSource fileDescriptorSource = clusterRegistry.get(REGISTRY_SCOPE, REGISTRY_KEY);
      if (fileDescriptorSource == null)
         return null;
      char[] data = fileDescriptorSource.getFileDescriptors().get(name);
      return data != null ? String.valueOf(data) : null;
   }

   public void registerProtofiles(String... classPathResources) throws Exception {
      FileDescriptorSource fileDescriptorSource = getFileDescriptorSource();
      for (String classPathResource : classPathResources) {
         String absPath = classPathResource.startsWith("/") ? classPathResource : "/" + classPathResource;
         String path = classPathResource.startsWith("/") ? classPathResource.substring(1) : classPathResource;
         fileDescriptorSource.addProtoFile(path, Util.getResourceAsStream(absPath, getClass().getClassLoader()));
      }
      clusterRegistry.put(REGISTRY_SCOPE, REGISTRY_KEY, fileDescriptorSource);
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

   private FileDescriptorSource getFileDescriptorSource() {
      FileDescriptorSource fileDescriptorSource = clusterRegistry.get(REGISTRY_SCOPE, REGISTRY_KEY);
      if (fileDescriptorSource == null) {
         fileDescriptorSource = new FileDescriptorSource();
      }
      return fileDescriptorSource;
   }

   @Listener
   class ProtobufMetadataRegistryListener {

      @CacheEntryCreated
      public void created(CacheEntryCreatedEvent<ScopedKey<String, String>, FileDescriptorSource> e) throws IOException {
         if (!e.isPre()) {
            registerProtofile(e.getValue());
         }
      }

      private void registerProtofile(FileDescriptorSource value) throws IOException {
         serCtx.registerProtoFiles(value);
      }

      @CacheEntryModified
      public void modified(CacheEntryModifiedEvent<ScopedKey<String, String>, FileDescriptorSource> e) throws IOException {
         if (!e.isPre()) {
            registerProtofile(e.getValue());
         }
      }
   }
}
