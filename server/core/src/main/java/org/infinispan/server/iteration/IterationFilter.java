package org.infinispan.server.iteration;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.marshall.protostream.impl.WrappedMessages;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author gustavonalle
 * @author wburns
 * @since 8.0
 */
@ProtoTypeId(ProtoStreamTypeIds.SERVER_ITERATION_FILTER)
@Scope(Scopes.NONE)
public class IterationFilter<K, V, C> extends AbstractKeyValueFilterConverter<K, V, C> {

   @ProtoField(1)
   final MediaType requestType;

   @ProtoField(2)
   final MediaType storageMediaType;

   final KeyValueFilterConverter<K, V, C> providedFilter;

   private transient Transcoder applyBefore, applyAfter;

   public IterationFilter(MediaType storageMediaType, MediaType requestType, KeyValueFilterConverter<K, V, C> providedFilter) {
      this.storageMediaType = storageMediaType;
      this.requestType = requestType;
      this.providedFilter = providedFilter;
   }

   @ProtoFactory
   IterationFilter(MediaType storageMediaType, MediaType requestType, WrappedMessage wrappedProvidedFilter) {
      this(storageMediaType, requestType, (KeyValueFilterConverter<K, V, C>) WrappedMessages.unwrap(wrappedProvidedFilter));
   }

   @ProtoField(value = 3, name = "providedFilter")
   WrappedMessage getWrappedProvidedFilter() {
      return WrappedMessages.orElseNull(providedFilter);
   }

   @Override
   public C filterAndConvert(K key, V value, Metadata metadata) {
      if (providedFilter != null) {
         Object keyTranscoded = key;
         Object valueTranscoded = value;
         if (applyBefore != null) {
            keyTranscoded = applyBefore.transcode(key, storageMediaType, providedFilter.format());
            valueTranscoded = applyBefore.transcode(value, storageMediaType, providedFilter.format());
         }

         C result = providedFilter.filterAndConvert((K) keyTranscoded, (V) valueTranscoded, metadata);
         if (result == null) return null;
         if (applyAfter == null) return result;
         return (C) applyAfter.transcode(result, providedFilter.format(), requestType);
      } else {
         return (C) value;
      }
   }

   @Inject
   public void injectDependencies(Cache cache, EncoderRegistry encoderRegistry) {
      if (providedFilter != null) {
         ComponentRegistry.of(cache).wireDependencies(providedFilter);
         MediaType filterFormat = providedFilter.format();
         if (filterFormat != null && !filterFormat.equals(storageMediaType)) {
            applyBefore = encoderRegistry.getTranscoder(filterFormat, storageMediaType);
         }
         if (filterFormat != null && !filterFormat.equals(requestType)) {
            applyAfter = encoderRegistry.getTranscoder(filterFormat, requestType);
         }
      }
   }
}
