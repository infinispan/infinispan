package org.infinispan.marshall.core;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.dataconversion.Wrapper;

/**
 * Manages existent {@link Encoder}, {@link Wrapper} and {@link Transcoder} instances.
 *
 * @since 9.1
 */
public interface EncoderRegistry {

   Encoder getEncoder(Class<? extends Encoder> encoderClass, short encoderId);

   boolean isRegistered(Class<? extends Encoder> encoderClass);

   Wrapper getWrapper(Class<? extends Wrapper> wrapperClass, byte wrapperId);

   /**
    * @param encoder {@link Encoder to be registered}.
    */
   void registerEncoder(Encoder encoder);

   void registerWrapper(Wrapper wrapper);

   void registerTranscoder(Transcoder transcoder);

   /**
    * Obtain an instance of {@link Transcoder} from the registry.
    *
    * @param type1 {@link MediaType} supported by the transcoder.
    * @param type2 {@link MediaType} supported by the transcoder.
    * @return An instance of {@link Transcoder} capable of doing conversions between the supplied MediaTypes.
    */
   Transcoder getTranscoder(MediaType type1, MediaType type2);

   <T extends Transcoder> T getTranscoder(Class<T> clazz);

   boolean isConversionSupported(MediaType from, MediaType to);
}
