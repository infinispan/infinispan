package org.infinispan.server.hotrod.iteration;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Optional;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.metadata.Metadata;

/**
 * @author gustavonalle
 * @author wburns
 * @since 8.0
 */

public class IterationFilter<K, V, C> extends AbstractKeyValueFilterConverter<K, V, C> {
   final Optional<KeyValueFilterConverter<K, V, C>> providedFilter;
   private final MediaType requestType;
   private final MediaType storageMediaType;
   private transient Transcoder applyBefore, applyAfter;

   public IterationFilter(MediaType storageMediaType, MediaType requestType, Optional<KeyValueFilterConverter<K, V, C>> providedFilter) {
      this.storageMediaType = storageMediaType;
      this.requestType = requestType;
      this.providedFilter = providedFilter;
   }

   @Override
   public C filterAndConvert(K key, V value, Metadata metadata) {
      if (providedFilter.isPresent()) {
         KeyValueFilterConverter<K, V, C> f = providedFilter.get();
         Object keyTranscoded = key;
         Object valueTranscoded = value;
         if (applyBefore != null) {
            keyTranscoded = applyBefore.transcode(key, storageMediaType, f.format());
            valueTranscoded = applyBefore.transcode(value, storageMediaType, f.format());
         }

         C result = f.filterAndConvert((K) keyTranscoded, (V) valueTranscoded, metadata);
         if (result == null) return null;
         if (applyAfter == null) return result;
         return (C) applyAfter.transcode(result, f.format(), requestType);
      } else {
         return (C) value;
      }
   }

   @Inject
   public void injectDependencies(Cache cache, EncoderRegistry encoderRegistry) {
      providedFilter.ifPresent(kvfc -> {
         cache.getAdvancedCache().getComponentRegistry().wireDependencies(kvfc);
         MediaType filterFormat = kvfc.format();
         if (filterFormat != null && !filterFormat.equals(storageMediaType)) {
            applyBefore = encoderRegistry.getTranscoder(filterFormat, storageMediaType);
         }
         if (filterFormat != null && !filterFormat.equals(requestType)) {
            applyAfter = encoderRegistry.getTranscoder(filterFormat, requestType);
         }
      });
   }

   public static class IterationFilterExternalizer extends AbstractExternalizer<IterationFilter> {
      @Override
      public Set<Class<? extends IterationFilter>> getTypeClasses() {
         return Util.asSet(IterationFilter.class);
      }

      @Override
      public void writeObject(ObjectOutput output, IterationFilter object) throws IOException {
         if (object.providedFilter.isPresent()) {
            output.writeBoolean(true);
            output.writeObject(object.providedFilter.get());
         } else {
            output.writeBoolean(false);
         }
         output.writeObject(object.storageMediaType);
         output.writeObject(object.requestType);
      }

      @Override
      public IterationFilter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Optional<KeyValueFilterConverter> filter;
         if (input.readBoolean()) {
            filter = Optional.of((KeyValueFilterConverter) input.readObject());
         } else {
            filter = Optional.empty();
         }
         MediaType storeType = (MediaType) input.readObject();
         MediaType requestType = (MediaType) input.readObject();
         return new IterationFilter(storeType, requestType, filter);
      }
   }
}
