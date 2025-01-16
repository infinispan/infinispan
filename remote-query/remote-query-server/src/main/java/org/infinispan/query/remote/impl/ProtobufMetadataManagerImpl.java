package org.infinispan.query.remote.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanException;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.internal.InternalCacheNames;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.UserContextInitializerImpl;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.configuration.global.GlobalAuthorizationConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.DescriptorParserException;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.protostream.types.java.CommonContainerTypesSchema;
import org.infinispan.protostream.types.java.CommonTypesSchema;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.query.remote.client.impl.MarshallerRegistration;
import org.infinispan.query.remote.impl.indexing.infinispan.InfinispanAnnotations;
import org.infinispan.query.remote.impl.indexing.search5.Search5Annotations;
import org.infinispan.query.remote.impl.logging.Log;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.security.impl.CreatePermissionConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@MBean(objectName = ProtobufMetadataManagerConstants.OBJECT_NAME,
      description = "Component that acts as a manager and persistent container for Protocol Buffers schema definitions in the scope of a CacheManger.")
@Scope(Scopes.GLOBAL)
public final class ProtobufMetadataManagerImpl implements ProtobufMetadataManager {

   private static final Log log = LogFactory.getLog(ProtobufMetadataManagerImpl.class, Log.class);

   private SerializationContext serCtx;

   private volatile Cache<String, String> protobufSchemaCache;

   @Inject
   EmbeddedCacheManager cacheManager;

   @Inject
   InternalCacheRegistry internalCacheRegistry;

   @Inject
   SerializationContextRegistry serializationContextRegistry;

   public ProtobufMetadataManagerImpl() {
   }

   void register(SerializationContextInitializer initializer) {
      initializer.registerSchema(getSerializationContext());
      initializer.registerMarshallers(getSerializationContext());
   }

   @Start
   void start() {
      GlobalConfiguration globalConfiguration = SecurityActions.getCacheManagerConfiguration(cacheManager);

      Configuration.Builder configuration = Configuration.builder();
      configuration.schemaValidation(globalConfiguration.serialization().schemaCompatibilityValidation())
            .wrapCollectionElements(true);
      Search5Annotations.configure(configuration);
      InfinispanAnnotations.configure(configuration);
      serCtx = ProtobufUtil.newSerializationContext(configuration.build());
      try {
         MarshallerRegistration.init(serCtx);
      } catch (DescriptorParserException e) {
         throw new CacheException("Failed to initialise the Protobuf serialization context", e);
      }
      register(new CommonTypesSchema());
      register(new CommonContainerTypesSchema());
      register(new UserContextInitializerImpl());

      internalCacheRegistry.registerInternalCache(PROTOBUF_METADATA_CACHE_NAME,
            getProtobufMetadataCacheConfig(globalConfiguration).build(),
            EnumSet.of(InternalCacheRegistry.Flag.USER, InternalCacheRegistry.Flag.PROTECTED, InternalCacheRegistry.Flag.PERSISTENT));

      Collection<SerializationContextInitializer> initializers = globalConfiguration.serialization().contextInitializers();
      if (initializers == null || initializers.isEmpty()) {
         initializers = ServiceFinder.load(SerializationContextInitializer.class, globalConfiguration.classLoader());
      }
      processSerializationContextInitializer(initializers);
   }

   private void processSerializationContextInitializer(Iterable<SerializationContextInitializer> initializers) {
      if (initializers != null) {
         for (SerializationContextInitializer sci : initializers) {
            log.debugf("Registering protostream serialization context initializer: %s", sci.getClass().getName());
            try {
               sci.registerSchema(serCtx);
               sci.registerMarshallers(serCtx);
            } catch (Exception e) {
               throw Log.CONTAINER.errorInitializingSerCtx(e);
            }
         }
      }
   }

   /**
    * Adds an interceptor to the protobuf manager cache to detect entry updates and sync the SerializationContext.
    *
    * @param cacheComponentRegistry the component registry of the protobuf manager cache
    */
   void addProtobufMetadataManagerInterceptor(BasicComponentRegistry cacheComponentRegistry) {
      ProtobufMetadataManagerInterceptor interceptor = new ProtobufMetadataManagerInterceptor();
      cacheComponentRegistry.registerComponent(ProtobufMetadataManagerInterceptor.class, interceptor, true);
      cacheComponentRegistry.addDynamicDependency(AsyncInterceptorChain.class.getName(), ProtobufMetadataManagerInterceptor.class.getName());
      cacheComponentRegistry.getComponent(AsyncInterceptorChain.class).wired().addInterceptorAfter(interceptor, EntryWrappingInterceptor.class);
   }

   /**
    * Adds a stop dependency on {@link InternalCacheNames#PROTOBUF_METADATA_CACHE_NAME} cache. This must be invoked for each cache that uses protobuf.
    *
    * @param dependantCacheName the name of the cache depending on the protobuf metadata cache
    */
   void addCacheDependency(String dependantCacheName) {
      SecurityActions.addCacheDependency(cacheManager, dependantCacheName, PROTOBUF_METADATA_CACHE_NAME);
   }

   /**
    * Get the protobuf schema cache (lazily).
    */
   public Cache<String, String> getCache() {
      if (protobufSchemaCache == null) {
         protobufSchemaCache = SecurityActions.getUnwrappedCache(cacheManager, PROTOBUF_METADATA_CACHE_NAME);
      }
      return protobufSchemaCache;
   }

   private static ConfigurationBuilder getProtobufMetadataCacheConfig(GlobalConfiguration globalConfiguration) {
      CacheMode cacheMode = globalConfiguration.isClustered() ? CacheMode.REPL_SYNC : CacheMode.LOCAL;

      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL).invocationBatching().enable()
            .transaction().lockingMode(LockingMode.PESSIMISTIC)
            .locking().isolationLevel(IsolationLevel.READ_COMMITTED).useLockStriping(false)
            .clustering().cacheMode(cacheMode)
            .stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(false)
            .encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE)
            .encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      GlobalAuthorizationConfiguration globalAuthz = globalConfiguration.security().authorization();
      if (globalAuthz.enabled()) {
         if (!globalAuthz.hasRole(SCHEMA_MANAGER_ROLE)) {
            globalAuthz.addRole(GlobalAuthorizationConfiguration.DEFAULT_ROLES.get(SCHEMA_MANAGER_ROLE));
         }
         AuthorizationConfigurationBuilder authorization = cfg.security().authorization().enable();
         // Copy all global roles
         globalAuthz.roles().keySet().forEach(role -> authorization.role(role));
         // Add a special module which translates permissions
         cfg.addModule(CreatePermissionConfigurationBuilder.class);
      }
      return cfg;
   }

   @Override
   public void registerMarshaller(BaseMarshaller<?> marshaller) {
      serCtx.registerMarshaller(marshaller);
      serializationContextRegistry.addMarshaller(SerializationContextRegistry.MarshallerType.GLOBAL, marshaller);
   }

   @Override
   public void unregisterMarshaller(BaseMarshaller<?> marshaller) {
      serCtx.unregisterMarshaller(marshaller);
   }

   @ManagedOperation(description = "Registers a Protobuf definition file", displayName = "Register a Protofile")
   @Override
   public void registerProtofile(@Parameter(name = "fileName", description = "the name of the .proto file") String fileName,
                                 @Parameter(name = "contents", description = "contents of the file") String contents) {
      getCache().put(fileName, contents);
   }

   @ManagedOperation(description = "Registers multiple Protobuf definition files", displayName = "Register Protofiles")
   @Override
   public void registerProtofiles(@Parameter(name = "fileNames", description = "names of the protofiles") String[] fileNames,
                                  @Parameter(name = "fileContents", description = "content of the files") String[] contents) throws Exception {
      if (fileNames.length != contents.length) {
         throw new MBeanException(new IllegalArgumentException("invalid parameter sizes"));
      }
      Map<String, String> files = new HashMap<>(fileNames.length);
      for (int i = 0; i < fileNames.length; i++) {
         files.put(fileNames[i], contents[i]);
      }
      getCache().putAll(files);
   }

   @ManagedOperation(description = "Unregisters a Protobuf definition files", displayName = "Unregister a Protofiles")
   @Override
   public void unregisterProtofile(@Parameter(name = "fileName", description = "the name of the .proto file") String fileName) {
      if (getCache().remove(fileName) == null) {
         throw new IllegalArgumentException("File does not exist : " + fileName);
      }
   }

   @ManagedOperation(description = "Unregisters multiple Protobuf definition files", displayName = "Unregister Protofiles")
   @Override
   public void unregisterProtofiles(@Parameter(name = "fileNames", description = "names of the protofiles") String[] fileNames) {
      for (String fileName : fileNames) {
         if (getCache().remove(fileName) == null) {
            throw new IllegalArgumentException("File does not exist : " + fileName);
         }
      }
   }

   @ManagedAttribute(description = "The names of all Protobuf files", displayName = "Protofile Names")
   @Override
   public String[] getProtofileNames() {
      List<String> fileNames = new ArrayList<>();
      for (String k : getCache().keySet()) {
         if (k.endsWith(PROTO_KEY_SUFFIX)) {
            fileNames.add(k);
         }
      }
      Collections.sort(fileNames);
      return fileNames.toArray(new String[fileNames.size()]);
   }

   @ManagedOperation(description = "Get the contents of a protobuf definition file", displayName = "Get Protofile")
   @Override
   public String getProtofile(@Parameter(name = "fileName", description = "the name of the .proto file") String fileName) {
      if (!fileName.endsWith(PROTO_KEY_SUFFIX)) {
         throw new IllegalArgumentException("The file name must have \".proto\" suffix");
      }
      String fileContents = getCache().get(fileName);
      if (fileContents == null) {
         throw new IllegalArgumentException("File does not exist : " + fileName);
      }
      return fileContents;
   }

   @ManagedAttribute(description = "The names of the files that have errors, if any", displayName = "Files With Errors")
   @Override
   public String[] getFilesWithErrors() {
      String filesWithErrors = getCache().get(ERRORS_KEY_SUFFIX);
      if (filesWithErrors == null) {
         return null;
      }
      String[] fileNames = filesWithErrors.split("\n");
      Arrays.sort(fileNames);
      return fileNames;
   }

   @ManagedOperation(description = "Obtains the errors associated with a protobuf definition file", displayName = "Get Errors For A File")
   @Override
   public String getFileErrors(@Parameter(name = "fileName", description = "the name of the .proto file") String fileName) {
      if (!fileName.endsWith(PROTO_KEY_SUFFIX)) {
         throw new IllegalArgumentException("The file name must have \".proto\" suffix");
      }
      if (!getCache().containsKey(fileName)) {
         throw new IllegalArgumentException("File does not exist : " + fileName);
      }
      return getCache().get(fileName + ERRORS_KEY_SUFFIX);
   }

   public Set<String> getKnownTypes() {
      return getSerializationContext().getGenericDescriptors().keySet();
   }

   SerializationContext getSerializationContext() {
      return serCtx;
   }

   /**
    * Obtains the ProtobufMetadataManagerImpl instance associated to a cache manager.
    *
    * @param cacheManager a cache manager instance
    * @return the ProtobufMetadataManagerImpl instance associated to a cache manager.
    */
   private static ProtobufMetadataManagerImpl getProtobufMetadataManager(EmbeddedCacheManager cacheManager) {
      if (cacheManager == null) {
         throw new IllegalArgumentException("cacheManager cannot be null");
      }
      ProtobufMetadataManagerImpl metadataManager = (ProtobufMetadataManagerImpl) GlobalComponentRegistry.componentOf(cacheManager, ProtobufMetadataManager.class);
      if (metadataManager == null) {
         throw new IllegalStateException("ProtobufMetadataManager not initialised yet!");
      }
      return metadataManager;
   }

   /**
    * Obtains the protobuf serialization context of the ProtobufMetadataManager instance associated to a cache manager.
    *
    * @param cacheManager a cache manager instance
    * @return the protobuf {@link SerializationContext}
    */
   public static SerializationContext getSerializationContext(EmbeddedCacheManager cacheManager) {
      return getProtobufMetadataManager(cacheManager).getSerializationContext();
   }
}
