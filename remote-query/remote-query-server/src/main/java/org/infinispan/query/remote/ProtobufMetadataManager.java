package org.infinispan.query.remote;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.DescriptorParserException;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.AnnotationElement;
import org.infinispan.query.remote.client.MarshallerRegistration;
import org.infinispan.query.remote.indexing.IndexingMetadata;
import org.infinispan.query.remote.indexing.IndexingMetadataCreator;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;

import javax.management.MBeanException;
import javax.management.ObjectName;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A clustered repository of protobuf descriptors. All protobuf types and their marshallers must be registered with this
 * repository before being used.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@Scope(Scopes.GLOBAL)
@MBean(objectName = ProtobufMetadataManager.OBJECT_NAME,
       description = "Component that acts as a manager and container for Protocol Buffers message type definitions in the scope of a CacheManger.")
public class ProtobufMetadataManager implements ProtobufMetadataManagerMBean {

   public static final String OBJECT_NAME = "ProtobufMetadataManager";

   /**
    * The name of the Protobuf definitions cache.
    */
   public static final String PROTOBUF_METADATA_CACHE_NAME = "___protobuf_metadata";

   private Cache<String, String> protobufSchemaCache;

   private ObjectName objectName;

   private final SerializationContext serCtx;

   private EmbeddedCacheManager cacheManager;

   public ProtobufMetadataManager() {
      org.infinispan.protostream.config.Configuration.Builder configBuilder = new org.infinispan.protostream.config.Configuration.Builder();
      configBuilder
            .messageAnnotation(IndexingMetadata.INDEXED_ANNOTATION)
               .attribute(AnnotationElement.Annotation.DEFAULT_ATTRIBUTE)
                  .booleanType()
                  .defaultValue(true)
               .annotationMetadataCreator(new IndexingMetadataCreator())
            .fieldAnnotation(IndexingMetadata.INDEXED_FIELD_ANNOTATION)
               .attribute("index")
                  .booleanType()
                  .defaultValue(true)
               .attribute("store")
                  .booleanType()
                  .defaultValue(true);
      serCtx = ProtobufUtil.newSerializationContext(configBuilder.build());
      try {
         MarshallerRegistration.registerMarshallers(serCtx);
      } catch (IOException | DescriptorParserException e) {
         throw new CacheException("Failed to initialise the Protobuf serialization context", e);
      }
   }

   @Inject
   public void init(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
      // define the cache configuration if it was not already defined by the user
      Configuration cacheConfiguration = cacheManager.getCacheConfiguration(PROTOBUF_METADATA_CACHE_NAME);
      if (cacheConfiguration == null) {
         cacheManager.defineConfiguration(PROTOBUF_METADATA_CACHE_NAME, getProtobufMetadataCacheConfig().build());
      } else {
         throw new IllegalStateException("A cache configuration named " + PROTOBUF_METADATA_CACHE_NAME
                                               + " already exists. This must not be configured externally by the user.");
      }
   }

   /**
    * Obtain the cache.
    */
   Cache<String, String> getCache() {
      if (protobufSchemaCache == null) {
         protobufSchemaCache = cacheManager.getCache(PROTOBUF_METADATA_CACHE_NAME);
      }
      return protobufSchemaCache;
   }

   private ConfigurationBuilder getProtobufMetadataCacheConfig() {
      GlobalConfiguration globalConfiguration = cacheManager.getGlobalComponentRegistry().getGlobalConfiguration();
      CacheMode cacheMode = globalConfiguration.isClustered() ? CacheMode.REPL_SYNC : CacheMode.LOCAL;

      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL).invocationBatching().enable()
            .transaction().lockingMode(LockingMode.PESSIMISTIC).syncCommitPhase(true).syncRollbackPhase(true)
            .locking().isolationLevel(IsolationLevel.READ_COMMITTED).useLockStriping(false)
            .clustering().cacheMode(cacheMode).sync()
            .stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(false)
            .compatibility().enable().marshaller(new CompatibilityProtoStreamMarshaller())
            .customInterceptors().addInterceptor()
            .interceptor(new ProtobufMetadataManagerInterceptor()).after(PessimisticLockingInterceptor.class);
      return cfg;
   }

   public ObjectName getObjectName() {
      return objectName;
   }

   public void setObjectName(ObjectName objectName) {
      this.objectName = objectName;
   }

   public void registerMarshaller(BaseMarshaller<?> marshaller) {
      serCtx.registerMarshaller(marshaller);
   }

   @ManagedOperation(description = "Registers a set of Protobuf definition files", displayName = "Register Protofiles")
   public void registerProtofiles(@Parameter(name = "fileNames", description = "names of the protofiles") String[] names,
                                  @Parameter(name = "fileContents", description = "content of the files") String[] contents)
         throws Exception {
      if (names.length != contents.length) {
         throw new MBeanException(new IllegalArgumentException("invalid parameter sizes"));
      }
      Map<String, String> files = new HashMap<String, String>(names.length);
      for (int i = 0; i < names.length; i++) {
         files.put(names[i], contents[i]);
      }
      getCache().putAll(files);
   }

   @ManagedOperation(description = "Registers a Protobuf definition file", displayName = "Register Protofile")
   public void registerProtofile(@Parameter(name = "fileName", description = "the name of the .proto file") String name,
                                 @Parameter(name = "contents", description = "contents of the file") String contents) {
      getCache().put(name, contents);
   }

   public void registerProtofiles(String... classPathResources) throws Exception {
      Map<String, String> files = new HashMap<String, String>(classPathResources.length);
      for (String classPathResource : classPathResources) {
         String absPath = classPathResource.startsWith("/") ? classPathResource : "/" + classPathResource;
         String path = classPathResource.startsWith("/") ? classPathResource.substring(1) : classPathResource;
         files.put(path, Util.read(Util.getResourceAsStream(absPath, getClass().getClassLoader())));
      }
      getCache().putAll(files);
   }

   @ManagedOperation(description = "Display a protobuf definition file", displayName = "Display Protofile")
   public String displayProtofile(@Parameter(name = "fileName", description = "the name of the .proto file") String fileName) {
      if (!fileName.endsWith(".proto")) {
         throw new IllegalArgumentException("The file name must have \".proto\" suffix");
      }
      return getCache().get(fileName);
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
      return new DelegatingSerializationContext(metadataManager);
   }

   public static SerializationContext getSerializationContextInternal(EmbeddedCacheManager cacheManager) {
      DelegatingSerializationContext delegatingCtx = (DelegatingSerializationContext) getSerializationContext(cacheManager);
      return delegatingCtx.getDelegate();
   }
}
