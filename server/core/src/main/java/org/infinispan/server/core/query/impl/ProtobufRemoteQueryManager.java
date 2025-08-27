package org.infinispan.server.core.query.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.encoding.impl.StorageConfigurationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.infinispan.query.objectfilter.Matcher;
import org.infinispan.query.objectfilter.impl.ProtobufMatcher;

/**
 * {@link RemoteQueryManager} suitable for caches storing protobuf.
 *
 * @since 9.2
 */
public final class ProtobufRemoteQueryManager extends BaseRemoteQueryManager {

   private final RemoteQueryEngine queryEngine;
   private final Transcoder protobufTranscoder;

   public ProtobufRemoteQueryManager(AdvancedCache<?, ?> cache, ComponentRegistry cr, SerializationContext serCtx,
                                     QuerySerializers querySerializers, SearchMapping searchMapping) {
      super(cache, querySerializers, cr);
      RemoteHibernateSearchPropertyHelper propertyHelper = RemoteHibernateSearchPropertyHelper.create(serCtx, searchMapping);
      Matcher matcher = new ObjectProtobufMatcher(serCtx, propertyHelper);
      cr.registerComponent(matcher, ProtobufMatcher.class);

      Configuration configuration = cache.getCacheConfiguration();
      boolean isIndexed = configuration.indexing().enabled();
      boolean customStorage = configuration.encoding().valueDataType().isMediaTypeChanged();
      MediaType valueMediaType = getValueDataConversion().getStorageMediaType();
      boolean isProtoBuf = valueMediaType.match(APPLICATION_PROTOSTREAM);
      if (isProtoBuf || !customStorage && isIndexed) {
         StorageConfigurationManager storageConfigurationManager = cr.getComponent(StorageConfigurationManager.class);
         storageConfigurationManager.overrideWrapper(storageConfigurationManager.getKeyWrapper(), ProtobufWrapper.INSTANCE);
      }
      this.queryEngine = new RemoteQueryEngine(cache, isIndexed);
      EncoderRegistry encoderRegistry = cr.getGlobalComponentRegistry().getComponent(EncoderRegistry.class);
      this.protobufTranscoder = encoderRegistry.getTranscoder(APPLICATION_PROTOSTREAM, APPLICATION_OBJECT);
   }

   @Override
   public Class<? extends Matcher> getMatcherClass(MediaType mediaType) {
      return ProtobufMatcher.class;
   }

   @Override
   public RemoteQueryEngine getQueryEngine(AdvancedCache<?, ?> cache) {
      return queryEngine;
   }

   @Override
   public Object encodeFilterResult(Object filterResult) {
      return protobufTranscoder.transcode(filterResult, APPLICATION_OBJECT, APPLICATION_PROTOSTREAM);
   }
}
