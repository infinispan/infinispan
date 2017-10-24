package org.infinispan.marshall.core;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.concurrent.ConcurrentHashSet;
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

   private final Map<Class<? extends Encoder>, Encoder> encoderMap = CollectionFactory.makeConcurrentMap(10);
   private final Map<Class<? extends Wrapper>, Wrapper> wrapperMap = CollectionFactory.makeConcurrentMap(2);
   private final Map<Short, Class<? extends Encoder>> encoderById = CollectionFactory.makeConcurrentMap(10);
   private final Map<Byte, Class<? extends Wrapper>> wrapperById = CollectionFactory.makeConcurrentMap(2);
   private final Set<Transcoder> transcoders = new ConcurrentHashSet<>();

   @Override
   public void registerEncoder(Encoder encoder) {
      if (encoder == null) {
         throw new IllegalArgumentException("Encoder cannot be null");
      }
      short id = encoder.id();
      if (encoderById.containsKey(id)) {
         throw log.duplicateIdEncoder(id);
      }
      encoderById.put(id, encoder.getClass());
      encoderMap.put(encoder.getClass(), encoder);
   }

   @Override
   public void registerWrapper(Wrapper wrapper) {
      if (wrapper == null) {
         throw new IllegalArgumentException("Wrapper cannot be null");
      }
      byte id = wrapper.id();
      if (wrapperById.containsKey(id)) {
         throw log.duplicateIdWrapper(id);
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
         throw log.cannotFindTranscoder(mediaType, another);
      }
      return transcoder.get();
   }

   @Override
   public Set<String> getSupportedMediaTypes() {
      return transcoders.stream().flatMap(t -> t.getSupportedMediaTypes().stream()).map(MediaType::getTypeSubtype).collect(Collectors.toSet());
   }

   @Override
   public Encoder getEncoder(Class<? extends Encoder> clazz, Short encoderId) {
      if (clazz == null && encoderId == null) {
         throw new IllegalArgumentException("Encoder class or identifier must be provided!");
      }
      Class<? extends Encoder> encoderClass = clazz == null ? encoderById.get(encoderId) : clazz;
      if (encoderClass == null) {
         throw log.encoderIdNotFound(encoderId);
      }
      Encoder encoder = encoderMap.get(encoderClass);
      if (encoder == null) {
         throw log.encoderClassNotFound(clazz);
      }
      return encoder;
   }

   @Override
   public Wrapper getWrapper(Class<? extends Wrapper> clazz, Byte wrapperId) {
      if (clazz == null && wrapperId == null) {
         throw new IllegalArgumentException("Wrapper class or identifier must be provided!");
      }
      Class<? extends Wrapper> wrapperClass = clazz == null ? wrapperById.get(wrapperId) : clazz;
      if (wrapperClass == null) {
         throw log.wrapperIdNotFound(wrapperId);
      }
      Wrapper wrapper = wrapperMap.get(wrapperClass);
      if (wrapper == null) {
         throw log.wrapperClassNotFound(clazz);
      }
      return wrapper;
   }

}
