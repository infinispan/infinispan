package org.infinispan.query.remote.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.QueryRequest;
import org.infinispan.query.remote.client.QueryResponse;
import org.infinispan.query.remote.impl.logging.Log;

/**
 * {@link RemoteQueryManager} suitable for caches storing protobuf.
 *
 * @since 9.2
 */
class ProtobufRemoteQueryManager implements RemoteQueryManager {

   private static final Log log = LogFactory.getLog(ProtobufRemoteQueryManager.class, Log.class);

   private final Matcher matcher;
   private final BaseRemoteQueryEngine queryEngine;
   private final SerializationContext serCtx;
   private final Encoder keyEncoder;
   private final Encoder valueEncoder;
   private final Transcoder transcoder;

   ProtobufRemoteQueryManager(SerializationContext serCtx, ComponentRegistry cr) {
      this.serCtx = serCtx;
      this.matcher = new ProtobufMatcher(serCtx, ProtobufFieldIndexingMetadata::new);
      cr.registerComponent(matcher, ProtobufMatcher.class);
      AdvancedCache<?, ?> cache = cr.getComponent(Cache.class).getAdvancedCache();
      boolean isIndexed = cr.getComponent(Configuration.class).indexing().index().isEnabled();
      if (isIndexed) {
         DataConversion valueDataConversion = cache.getAdvancedCache().getValueDataConversion();
         valueDataConversion.overrideWrapper(ProtostreamWrapper.class, cr);
      }
      this.queryEngine = new RemoteQueryEngine(cache, isIndexed);
      this.keyEncoder = cache.getKeyDataConversion().getEncoder();
      this.valueEncoder = cache.getValueDataConversion().getEncoder();
      this.transcoder = cr.getGlobalComponentRegistry().getComponent(EncoderRegistry.class)
            .getTranscoder(APPLICATION_PROTOSTREAM, APPLICATION_JSON);
   }

   @Override
   public Matcher getMatcher() {
      return matcher;
   }

   @Override
   public BaseRemoteQueryEngine getQueryEngine() {
      return queryEngine;
   }

   @Override
   public QueryRequest decodeQueryRequest(byte[] queryRequest) {
      try {
         return ProtobufUtil.fromByteArray(queryEngine.getSerializationContext(), queryRequest, 0, queryRequest.length, QueryRequest.class);
      } catch (IOException e) {
         throw log.errorExecutingQuery(e);
      }
   }

   @Override
   public byte[] encodeQueryResponse(QueryResponse queryResponse) {
      try {
         return ProtobufUtil.toByteArray(queryEngine.getSerializationContext(), queryResponse);
      } catch (IOException e) {
         throw log.errorExecutingQuery(e);
      }
   }

   @Override
   public Object encodeFilterResult(Object filterResult) {
      try {
         return ProtobufUtil.toWrappedByteArray(serCtx, filterResult);
      } catch (IOException e) {
         throw log.errorFiltering(e);
      }
   }

   @Override
   public Encoder getKeyEncoder() {
      return keyEncoder;
   }

   @Override
   public Encoder getValueEncoder() {
      return valueEncoder;
   }

   @Override
   public List<Object> encodeQueryResults(List<Object> results) {
      return results.stream().map(o -> transcoder.transcode(o, APPLICATION_PROTOSTREAM, APPLICATION_JSON)).collect(Collectors.toList());
   }
}
