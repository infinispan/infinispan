package org.infinispan.stream.impl.intops;

import java.util.stream.BaseStream;

/**
 * Performs unordered operation on a {@link BaseStream}
 * @param <Type> the type of the stream
 * @param <Stream> the stream type
 */
public class UnorderedOperation<Type, Stream extends BaseStream<Type, Stream>>
        implements IntermediateOperation<Type, Stream, Type, Stream> {
   @Override
   public BaseStream perform(BaseStream stream) {
      return stream.unordered();
   }
}
