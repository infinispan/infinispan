package org.infinispan.stream.impl.intops;

import org.infinispan.factories.ComponentRegistry;

import java.util.stream.BaseStream;

/**
 * Intermediate operation that can be applied to a stream to change its processing.
 * @param <InputType> the type of the input stream
 * @param <InputStream> the input stream type
 * @param <OutputType> the type of the output stream
 * @param <OutputStream> the output stream type
 */
public interface IntermediateOperation<InputType, InputStream extends BaseStream<InputType, InputStream>,
        OutputType, OutputStream extends BaseStream<OutputType, OutputStream>> {
   /**
    * Performs the actualy intermediate operation returning the resulting stream
    * @param stream the stream to have the operation performed on
    * @return the resulting stream after the operation was applied
    */
   OutputStream perform(InputStream stream);

   /**
    * Handles injection of components for various dependencies that the intermediate operation has
    * @param registry the registry to use
    */
   default void handleInjection(ComponentRegistry registry) {
      // Default is nothing is done
   }
}
