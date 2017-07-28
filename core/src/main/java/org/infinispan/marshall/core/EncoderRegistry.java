package org.infinispan.marshall.core;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;

/**
 * Manages existent {@link Encoder} and {@link Wrapper} instances.
 *
 * @since 9.1
 */
public interface EncoderRegistry {

   Encoder getEncoder(Class<? extends Encoder> encoderClass, Short encoderId);

   Wrapper getWrapper(Class<? extends Wrapper> wrapperClass, Byte wrapperId);

   /**
    * @param encoder {@link Encoder to be registered}.
    */
   void registerEncoder(Encoder encoder);

   void registerWrapper(Wrapper wrapper);

}
