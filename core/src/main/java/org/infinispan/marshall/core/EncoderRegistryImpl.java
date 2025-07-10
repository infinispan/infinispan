package org.infinispan.marshall.core;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.infinispan.util.logging.Log.CONTAINER;

/**
 * @see EncoderRegistry
 * @since 9.1
 */
@Scope(Scopes.GLOBAL)
public class EncoderRegistryImpl implements EncoderRegistry {
   private final Map<Byte, Wrapper> wrapperMap = new ConcurrentHashMap<>();
   private final List<Transcoder> transcoders = Collections.synchronizedList(new ArrayList<>());
   private final Map<MediaType, Map<MediaType, Transcoder>> transcoderCache = new ConcurrentHashMap<>();

   public void registerWrapper(Wrapper wrapper) {
      if (wrapper == null) {
         throw new NullPointerException("Wrapper cannot be null");
      }
      byte id = wrapper.id();
      if (wrapperMap.containsKey(id)) {
         throw CONTAINER.duplicateIdWrapper(id);
      }
      wrapperMap.put(id, wrapper);
   }

   @Override
   public void registerTranscoder(Transcoder transcoder) {
      transcoders.add(transcoder);
   }

   @Override
   public Transcoder getTranscoder(MediaType mediaType, MediaType another) {
      Transcoder transcoder = getTranscoderOrNull(mediaType, another);
      if (transcoder == null) {
         throw CONTAINER.cannotFindTranscoder(mediaType, another);
      }
      return transcoder;
   }

   private Transcoder getTranscoderOrNull(MediaType mediaType, MediaType another) {
      return transcoderCache.computeIfAbsent(mediaType, mt -> new ConcurrentHashMap<>(4))
                            .computeIfAbsent(another, mt -> {
                               return transcoders.stream()
                                                 .filter(t -> t.supportsConversion(mediaType, another))
                                                 .findFirst()
                                                 .orElse(null);
                            });
   }

   @Override
   public <T extends Transcoder> T getTranscoder(Class<T> clazz) {
      Transcoder transcoder = transcoders.stream()
                                         .filter(p -> p.getClass() == clazz)
                                         .findAny()
                                         .orElse(null);
      return clazz.cast(transcoder);
   }

   @Override
   public boolean isConversionSupported(MediaType from, MediaType to) {
      if (from == null || to == null) {
         throw new NullPointerException("MediaType must not be null!");
      }
      return from.match(to) || getTranscoderOrNull(from, to) != null;
   }

   @Override
   public Object convert(Object o, MediaType from, MediaType to) {
      if (o == null) return null;
      Transcoder transcoder = getTranscoder(from, to);
      return transcoder.transcode(o, from, to);
   }
}
