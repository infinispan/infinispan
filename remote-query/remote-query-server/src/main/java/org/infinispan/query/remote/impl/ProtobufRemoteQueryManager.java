package org.infinispan.query.remote.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.QueryRequest;
import org.infinispan.query.remote.client.QueryResponse;

/**
 * {@link RemoteQueryManager} suitable for caches storing protobuf.
 *
 * @since 9.2
 */
class ProtobufRemoteQueryManager implements RemoteQueryManager {
   private final BaseRemoteQueryEngine queryEngine;
   private final DataConversion keyDataConversion;
   private final DataConversion valueDataConversion;
   private final Transcoder protobufTranscoder;

   ProtobufRemoteQueryManager(SerializationContext serCtx, ComponentRegistry cr) {
      Matcher matcher = new ProtobufMatcher(serCtx, ProtobufFieldIndexingMetadata::new);
      AdvancedCache<?, ?> cache = cr.getComponent(Cache.class).getAdvancedCache();
      Configuration configuration = cache.getCacheConfiguration();
      this.keyDataConversion = cache.getKeyDataConversion();
      this.valueDataConversion = cache.getValueDataConversion();
      cr.registerComponent(matcher, ProtobufMatcher.class);

      boolean isIndexed = configuration.indexing().index().isEnabled();
      boolean customStorage = cache.getCacheConfiguration().encoding().valueDataType().isMediaTypeChanged();
      MediaType valueMediaType = valueDataConversion.getStorageMediaType();
      boolean isProtoBuf = valueMediaType.match(APPLICATION_PROTOSTREAM);
      if (isProtoBuf || (!customStorage && isIndexed)) {
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
   public QueryRequest decodeQueryRequest(byte[] queryRequest, MediaType mediaType) {
      return (QueryRequest) protobufTranscoder.transcode(queryRequest, APPLICATION_PROTOSTREAM, QUERY_REQUEST_TYPE);
   }

   @Override
   public byte[] encodeQueryResponse(QueryResponse queryResponse, MediaType mediaType) {
      return (byte[]) protobufTranscoder.transcode(queryResponse, APPLICATION_OBJECT, PROTOSTREAM_UNWRAPPED);
   }

   @Override
   public Object encodeFilterResult(Object filterResult) {
      return protobufTranscoder.transcode(filterResult, APPLICATION_OBJECT, APPLICATION_PROTOSTREAM);
   }

   @Override
   public DataConversion getKeyDataConversion() {
      return keyDataConversion;
   }

   @Override
   public DataConversion getValueDataConversion() {
      return valueDataConversion;
   }

}
