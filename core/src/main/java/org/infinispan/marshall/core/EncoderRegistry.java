package org.infinispan.marshall.core;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;

/**
 * Manages {@link Transcoder} instances.
 *
 * @since 9.1
 */
public interface EncoderRegistry {

   /**
    * Registers a transcoder in the registry
    *
    * @param transcoder the transcoder instance to register
    */
   void registerTranscoder(Transcoder transcoder);

   /**
    * Retrieves an instance of {@link Transcoder} from the registry.
    *
    * @param type1 {@link MediaType} supported by the transcoder.
    * @param type2 {@link MediaType} supported by the transcoder.
    * @return An instance of {@link Transcoder} capable of doing conversions between the supplied MediaTypes.
    */
   Transcoder getTranscoder(MediaType type1, MediaType type2);

   /**
    * Looks up a {@link Transcoder} in the registry
    *
    * @param clazz the class of the transcoder
    * @param <T>   the specific transcoder implementation type
    * @return the registered instance of the transcoder
    */
   <T extends Transcoder> T getTranscoder(Class<T> clazz);

   /**
    * Returns whether conversion between specific {@link MediaType}s is supported
    *
    * @param from the source {@link MediaType}
    * @param to   the destination {@link MediaType}
    * @return true if conversion between the specified types is supported
    */
   boolean isConversionSupported(MediaType from, MediaType to);

   /**
    * Performs a data conversion.
    *
    * @param o    object to convert
    * @param from the object MediaType
    * @param to   the format to convert to
    * @return the object converted.
    * @since 11.0
    */
   Object convert(Object o, MediaType from, MediaType to);
}
