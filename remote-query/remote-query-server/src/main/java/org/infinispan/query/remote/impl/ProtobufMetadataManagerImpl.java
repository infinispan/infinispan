package org.infinispan.query.remote.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.management.MBeanException;
import javax.management.ObjectName;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.DescriptorParserException;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.query.remote.client.impl.MarshallerRegistration;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@MBean(objectName = ProtobufMetadataManagerConstants.OBJECT_NAME,
      description = "Component that acts as a manager and container for Protocol Buffers message type definitions in the scope of a CacheManger.")
@Scope(Scopes.GLOBAL)
public final class ProtobufMetadataManagerImpl implements ProtobufMetadataManager {

   private volatile Cache<String, String> protobufSchemaCache;

   private ObjectName objectName;

   private final SerializationContext serCtx;

   @Inject EmbeddedCacheManager cacheManager;

   public ProtobufMetadataManagerImpl() {
      Configuration.Builder cfgBuilder = Configuration.builder();
      IndexingMetadata.configure(cfgBuilder);
      serCtx = ProtobufUtil.newSerializationContext(cfgBuilder.build());
      try {
         MarshallerRegistration.init(serCtx);
      } catch (IOException | DescriptorParserException e) {
         throw new CacheException("Failed to initialise the Protobuf serialization context", e);
      }
   }

   /**
    * Starts the ___protobuf_metadata cache when needed. This method must be invoked for each cache that uses protobuf.
    *
    * @param dependantCacheName the name of the cache depending on the protobuf metadata cache
    */
   protected void addCacheDependency(String dependantCacheName) {
      protobufSchemaCache = (Cache<String, String>) SecurityActions.getUnwrappedCache(cacheManager, PROTOBUF_METADATA_CACHE_NAME).getAdvancedCache().withEncoding(IdentityEncoder.class);
      // add stop dependency
      cacheManager.addCacheDependency(dependantCacheName, ProtobufMetadataManagerImpl.PROTOBUF_METADATA_CACHE_NAME);
   }

   /**
    * Obtain the cache, lazily.
    */
   private Cache<String, String> getCache() {
      if (protobufSchemaCache == null) {
         throw new IllegalStateException("Not started yet");
      }
      return protobufSchemaCache;
   }

   @Override
   public ObjectName getObjectName() {
      return objectName;
   }

   void setObjectName(ObjectName objectName) {
      this.objectName = objectName;
   }

   @Override
   public void registerMarshaller(BaseMarshaller<?> marshaller) {
      serCtx.registerMarshaller(marshaller);
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
      ProtobufMetadataManagerImpl metadataManager = (ProtobufMetadataManagerImpl) cacheManager.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);
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
