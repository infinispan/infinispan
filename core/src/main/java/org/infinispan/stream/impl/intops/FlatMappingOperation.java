package org.infinispan.stream.impl.intops;

import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * Interface to signify that an {@link IntermediateOperation} is a flat map operation. This also provides proper
 * generics for converting a flat map as a map operation resulting in a Stream containing the proper stream
 * @author wburns
 * @since 9.0
 */
public interface FlatMappingOperation<InputType, InputStream extends BaseStream<InputType, InputStream>,
      OutputType, OutputStream extends BaseStream<OutputType, OutputStream>>
      extends MappingOperation<InputType, InputStream, OutputType, OutputStream> {

   /**
    * Instead of flat mapping this returns a stream of {@link OutputStream}.
    * @param inputStream the stream to convert
    * @return the stream of streams
    */
   Stream<OutputStream> map(InputStream inputStream);
}
