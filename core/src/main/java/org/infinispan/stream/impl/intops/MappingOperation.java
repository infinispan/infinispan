package org.infinispan.stream.impl.intops;

import java.util.stream.BaseStream;

/**
 * Marker interface to signify that an {@link IntermediateOperation} is a map operation.
 * @author wburns
 * @since 9.0
 */
public interface MappingOperation<InputType, InputStream extends BaseStream<InputType, InputStream>,
      OutputType, OutputStream extends BaseStream<OutputType, OutputStream>>
      extends IntermediateOperation<InputType, InputStream, OutputType, OutputStream> {
}
