package org.infinispan.query.remote.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.configuration.cache.Configuration;
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
class ProtobufRemoteQueryManager extends BaseRemoteQueryManager {

   private final RemoteQueryEngine queryEngine;
   private final Transcoder protobufTranscoder;

   ProtobufRemoteQueryManager(SerializationContext serCtx, ComponentRegistry cr, QuerySerializers querySerializers) {
      super(cr, querySerializers);
      Matcher matcher = new ProtobufMatcher(serCtx, ProtobufFieldIndexingMetadata::new);
      cr.registerComponent(matcher, ProtobufMatcher.class);

      Configuration configuration = cache.getCacheConfiguration();
      boolean isIndexed = configuration.indexing().index().isEnabled();
      boolean customStorage = configuration.encoding().valueDataType().isMediaTypeChanged();
      MediaType valueMediaType = valueDataConversion.getStorageMediaType();
      boolean isProtoBuf = valueMediaType.match(APPLICATION_PROTOSTREAM);
      if (isProtoBuf || !customStorage && isIndexed) {
         valueDataConversion.overrideWrapper(ProtobufWrapper.class, cr);
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
   public BaseRemoteQueryEngine getQueryEngine(AdvancedCache<?, ?> cache) {
      return queryEngine;
   }

   @Override
   public Object encodeFilterResult(Object filterResult) {
      return protobufTranscoder.transcode(filterResult, APPLICATION_OBJECT, APPLICATION_PROTOSTREAM);
   }
}
