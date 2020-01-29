package org.infinispan.marshall.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.EncoderIds;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.dataconversion.WrapperIds;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @see EncoderRegistry
 * @since 9.1
 */
@Scope(Scopes.GLOBAL)
public class EncoderRegistryImpl implements EncoderRegistry {
   private static final Log log = LogFactory.getLog(EncoderRegistryImpl.class);

   private final Map<Short, Encoder> encoderMap = new ConcurrentHashMap<>();
   private final Map<Byte, Wrapper> wrapperMap = new ConcurrentHashMap<>();
   private final List<Transcoder> transcoders = Collections.synchronizedList(new ArrayList<>());
   private final Map<MediaType, Map<MediaType, Transcoder>> transcoderCache = new ConcurrentHashMap<>();

   @Override
   public void registerEncoder(Encoder encoder) {
      if (encoder == null) {
         throw new NullPointerException("Encoder cannot be null");
      }
      short id = encoder.id();
      if (encoderMap.containsKey(id)) {
         throw log.duplicateIdEncoder(id);
      }
      encoderMap.put(id, encoder);
   }

   @Override
   public void registerWrapper(Wrapper wrapper) {
      if (wrapper == null) {
         throw new NullPointerException("Wrapper cannot be null");
      }
      byte id = wrapper.id();
      if (wrapperMap.containsKey(id)) {
         throw log.duplicateIdWrapper(id);
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
         throw log.cannotFindTranscoder(mediaType, another);
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
   public boolean isConversionSupported(MediaType from, MediaType to) {
      if (from == null || to == null) {
         throw new NullPointerException("MediaType must not be null!");
      }
      return from.match(to) || getTranscoderOrNull(from, to) != null;
   }

   @Override
   public Encoder getEncoder(Class<? extends Encoder> clazz, short encoderId) {
      if (clazz == null && encoderId == EncoderIds.NO_ENCODER) {
         throw new NullPointerException("Encoder class or identifier must be provided!");
      }

      Encoder encoder;
      if (encoderId != EncoderIds.NO_ENCODER) {
         encoder = encoderMap.get(encoderId);
         if (encoder == null) {
            throw log.encoderIdNotFound(encoderId);
         }
      } else {
         encoder = encoderMap.values().stream()
                             .filter(e -> e.getClass().equals(clazz))
                             .findFirst()
                             .orElse(null);
         if (encoder == null) {
            throw log.encoderClassNotFound(clazz);
         }
      }
      return encoder;
   }

   @Override
   public boolean isRegistered(Class<? extends Encoder> encoderClass) {
      return encoderMap.values().stream().anyMatch(e -> e.getClass().equals(encoderClass));
   }

   @Override
   public Wrapper getWrapper(Class<? extends Wrapper> clazz, byte wrapperId) {
      if (clazz == null && wrapperId == WrapperIds.NO_WRAPPER) {
         throw new NullPointerException("Wrapper class or identifier must be provided!");
      }

      Wrapper wrapper;
      if (wrapperId != WrapperIds.NO_WRAPPER) {
         wrapper = wrapperMap.get(wrapperId);
         if (wrapper == null) {
            throw log.wrapperIdNotFound(wrapperId);
         }
      } else {
         wrapper = wrapperMap.values().stream()
                             .filter(e -> e.getClass().equals(clazz))
                             .findAny()
                             .orElse(null);
         if (wrapper == null) {
            throw log.wrapperClassNotFound(clazz);
         }
      }
      return wrapper;
   }

}
