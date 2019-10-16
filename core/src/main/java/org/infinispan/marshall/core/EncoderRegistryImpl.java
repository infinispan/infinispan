package org.infinispan.marshall.core;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * @see EncoderRegistry
 * @since 9.1
 */
@Scope(Scopes.GLOBAL)
public class EncoderRegistryImpl implements EncoderRegistry {
   private final Map<Class<? extends Encoder>, Encoder> encoderMap = new ConcurrentHashMap<>(10);
   private final Map<Class<? extends Wrapper>, Wrapper> wrapperMap = new ConcurrentHashMap<>(2);
   private final Map<Short, Class<? extends Encoder>> encoderById = new ConcurrentHashMap<>(10);
   private final Map<Byte, Class<? extends Wrapper>> wrapperById = new ConcurrentHashMap<>(2);
   private final Set<Transcoder> transcoders = ConcurrentHashMap.newKeySet();

   @Override
   public void registerEncoder(Encoder encoder) {
      if (encoder == null) {
         throw new NullPointerException("Encoder cannot be null");
      }
      short id = encoder.id();
      if (encoderById.containsKey(id)) {
         throw CONTAINER.duplicateIdEncoder(id);
      }
      encoderById.put(id, encoder.getClass());
      encoderMap.put(encoder.getClass(), encoder);
   }

   @Override
   public void registerWrapper(Wrapper wrapper) {
      if (wrapper == null) {
         throw new NullPointerException("Wrapper cannot be null");
      }
      byte id = wrapper.id();
      if (wrapperById.containsKey(id)) {
         throw CONTAINER.duplicateIdWrapper(id);
      }
      wrapperById.put(id, wrapper.getClass());
      wrapperMap.put(wrapper.getClass(), wrapper);
   }

   @Override
   public void registerTranscoder(Transcoder transcoder) {
      transcoders.add(transcoder);
   }

   @Override
   public Transcoder getTranscoder(MediaType mediaType, MediaType another) {
      Optional<Transcoder> transcoder = transcoders
            .stream()
            .filter(t -> t.supportsConversion(mediaType, another))
            .findAny();
      if (!transcoder.isPresent()) {
         throw CONTAINER.cannotFindTranscoder(mediaType, another);
      }
      return transcoder.get();
   }

   @Override
   public <T extends Transcoder> T getTranscoder(Class<T> clazz) {
      Transcoder transcoder = transcoders.stream().filter(p -> p.getClass().equals(clazz)).findFirst().orElse(null);
      if (transcoder == null) return null;
      return clazz.cast(transcoder);
   }

   @Override
   public boolean isConversionSupported(MediaType from, MediaType to) {
      if (from == null || to == null) {
         throw new NullPointerException("MediaType must not be null!");
      }
      return from.match(to) || transcoders.stream().anyMatch(t -> t.supportsConversion(from, to));
   }

   @Override
   public Encoder getEncoder(Class<? extends Encoder> clazz, Short encoderId) {
      if (clazz == null && encoderId == null) {
         throw new NullPointerException("Encoder class or identifier must be provided!");
      }
      Class<? extends Encoder> encoderClass = clazz == null ? encoderById.get(encoderId) : clazz;
      if (encoderClass == null) {
         throw CONTAINER.encoderIdNotFound(encoderId);
      }
      Encoder encoder = encoderMap.get(encoderClass);
      if (encoder == null) {
         throw CONTAINER.encoderClassNotFound(clazz);
      }
      return encoder;
   }

   @Override
   public boolean isRegistered(Class<? extends Encoder> encoderClass) {
      return encoderMap.containsKey(encoderClass);
   }

   @Override
   public Wrapper getWrapper(Class<? extends Wrapper> clazz, Byte wrapperId) {
      if (clazz == null && wrapperId == null) {
         throw new NullPointerException("Wrapper class or identifier must be provided!");
      }
      Class<? extends Wrapper> wrapperClass = clazz == null ? wrapperById.get(wrapperId) : clazz;
      if (wrapperClass == null) {
         throw CONTAINER.wrapperIdNotFound(wrapperId);
      }
      Wrapper wrapper = wrapperMap.get(wrapperClass);
      if (wrapper == null) {
         throw CONTAINER.wrapperClassNotFound(clazz);
      }
      return wrapper;
   }

}
