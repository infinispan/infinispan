package org.infinispan.container.entries;

import org.infinispan.io.ExpandableMarshalledValueByteStream;
import org.infinispan.io.ImmutableMarshalledValueByteStream;
import org.infinispan.io.MarshalledValueByteStream;
import org.infinispan.marshall.core.MarshalledValue;
import sun.misc.Unsafe;

/**
 * Entry Size calculator that returns an approximation of how much memory a marshalled value contains for a key
 * and value.  Note that this also handles any types that MarshalledValue doesn't as well.
 * @author wburns
 * @since 8.0
 */
public class MarshalledValueEntrySizeCalculator extends PrimitiveEntrySizeCalculator {
   @Override
   protected long handleObject(Object object) {
      Class<?> objClass = object.getClass();
      if (objClass == MarshalledValue.class) {
         MarshalledValue marshalledValue = (MarshalledValue)object;
         // Marshalled value is an object and reference to class
         long size = OBJECT_SIZE + POINTER_SIZE;
         // Marshalled value has MarshalledValueByteStream & StreamingMarshaller instance fields and 2 ints
         size = roundUpToNearest8(size + POINTER_SIZE * 2 + 4 * 2);
         // We ignored the StreamingMarshaller instance as it is shared
         MarshalledValueByteStream mvbs = marshalledValue.getRaw();
         long mvbsSize;
         if (mvbs instanceof ImmutableMarshalledValueByteStream) {
            // Each object has the normal Object overhead plus an additional overhead to have a reference to its
            // class definition
            mvbsSize = OBJECT_SIZE + POINTER_SIZE;
            // 1 variable in ImmutableMarshalledValueByteStream, which is a pointer to a byte[]
            mvbsSize += POINTER_SIZE;
            mvbsSize = roundUpToNearest8(mvbsSize);
         } else if (mvbs instanceof ExpandableMarshalledValueByteStream) {
            // Each object has the normal Object overhead plus an additional overhead to have a reference to its
            // class definition
            mvbsSize = OBJECT_SIZE + POINTER_SIZE;
            // 3 variables in ExpandableMarshalledValueByteStream, 1 is a pointer to a byte[]
            mvbsSize += POINTER_SIZE;
            // 1: the byte[] is calculated below
            // 2: the count as an int of how many bytes are written in the byte[]
            mvbsSize += 4;
            // 3: the doubling max size as an int that tells what the cutoff is before doing 25% increments versus 100%
            mvbsSize += 4;
            mvbsSize = roundUpToNearest8(mvbsSize);
         } else {
            throw new IllegalArgumentException(getClass() + " doesn't support counting for " + mvbs.getClass());
         }
         // Both implementations have a byte[] inside, so we need to add that to the size
         byte[] bytes = mvbs.getRaw();
         // Note that the byte[] takes up its own 8 aligned worth of size since it is a different instance
         size += mvbsSize + roundUpToNearest8(Unsafe.ARRAY_BYTE_BASE_OFFSET +
                 Unsafe.ARRAY_BYTE_INDEX_SCALE * bytes.length);
         return size;
      }
      return super.handleObject(object);
   }
}
