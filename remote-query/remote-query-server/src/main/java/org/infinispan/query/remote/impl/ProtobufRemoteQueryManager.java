package org.infinispan.query.remote.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.encoding.impl.StorageConfigurationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.protostream.SerializationContext;

/**
 * {@link RemoteQueryManager} suitable for caches storing protobuf.
 *
 * @since 9.2
 */
final class ProtobufRemoteQueryManager extends BaseRemoteQueryManager {

   private final RemoteQueryEngine queryEngine;
   private final Transcoder protobufTranscoder;

   ProtobufRemoteQueryManager(AdvancedCache<?, ?> cache, ComponentRegistry cr, SerializationContext serCtx, QuerySerializers querySerializers) {
      super(cache, querySerializers, cr);
      Matcher matcher = new ObjectProtobufMatcher(serCtx, ProtobufFieldIndexingMetadata::new, cache);
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
