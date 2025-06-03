package org.infinispan.marshall.core;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.encoding.DataConversion;

/**
 * Manages existent {@link Encoder}, {@link Wrapper} and {@link Transcoder} instances.
 *
 * @since 9.1
 */
public interface EncoderRegistry {

   @Deprecated(forRemoval=true, since = "11.0")
   Encoder getEncoder(Class<? extends Encoder> encoderClass, short encoderId);

   @Deprecated(forRemoval=true, since = "11.0")
   boolean isRegistered(Class<? extends Encoder> encoderClass);

   /**
    * @deprecated Since 11.0. To be removed in 14.0, with {@link DataConversion#getWrapper()}
    */
   @Deprecated(forRemoval=true, since = "11.0")
   Wrapper getWrapper(Class<? extends Wrapper> wrapperClass, byte wrapperId);

   /**
    * @deprecated Since 11.0. To be removed in 14.0, with {@link DataConversion#getWrapper()}
    */
   @Deprecated(forRemoval=true, since = "11.0")
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

   /**
    * Performs a data conversion.
    *
    * @param o object to convert
    * @param from the object MediaType
    * @param to the format to convert to
    * @return the object converted.
    * @since 11.0
    */
   Object convert(Object o, MediaType from, MediaType to);
}
