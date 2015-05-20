package org.infinispan.stream.impl.termop;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.object.FlatMapIteratorOperation;
import org.infinispan.stream.impl.termop.object.ForEachOperation;
import org.infinispan.stream.impl.termop.object.MapIteratorOperation;
import org.infinispan.stream.impl.termop.object.NoMapIteratorOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachDoubleOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachFlatMapDoubleOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachFlatMapIntOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachFlatMapLongOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachIntOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachLongOperation;
import org.jboss.marshalling.util.IdentityIntMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

/**
 * {@link AdvancedExternalizer} that provides functionality required for marshalling all of the various terminal
 * operations that are used by various distributed streams including the primitive forms.
 */
public class TerminalOperationExternalizer implements AdvancedExternalizer<BaseTerminalOperation> {
   private static final int SINGLE = 0;
   private static final int SEGMENT_RETRYING = 1;
   private static final int FLATMAP_ITERATOR = 2;
   private static final int MAP_ITERATOR = 3;
   private static final int NOMAP_ITERATOR = 4;
   private static final int FOREACH = 5;
   private static final int FOREACH_FLAT_DOUBLE = 6;
   private static final int FOREACH_FLAT_INT = 7;
   private static final int FOREACH_FLAT_LONG = 8;
   private static final int FOREACH_DOUBLE = 9;
   private static final int FOREACH_INT = 10;
   private static final int FOREACH_LONG = 11;

   private final IdentityIntMap<Class<? extends BaseTerminalOperation>> operations = new IdentityIntMap<>();

   public TerminalOperationExternalizer() {
      operations.put(SingleRunOperation.class, SINGLE);
      operations.put(SegmentRetryingOperation.class, SEGMENT_RETRYING);
      operations.put(FlatMapIteratorOperation.class, FLATMAP_ITERATOR);
      operations.put(MapIteratorOperation.class, MAP_ITERATOR);
      operations.put(NoMapIteratorOperation.class, NOMAP_ITERATOR);
      operations.put(ForEachOperation.class, FOREACH);
      operations.put(ForEachFlatMapDoubleOperation.class, FOREACH_FLAT_DOUBLE);
      operations.put(ForEachFlatMapIntOperation.class, FOREACH_FLAT_INT);
      operations.put(ForEachFlatMapLongOperation.class, FOREACH_FLAT_LONG);
      operations.put(ForEachDoubleOperation.class, FOREACH_DOUBLE);
      operations.put(ForEachIntOperation.class, FOREACH_INT);
      operations.put(ForEachLongOperation.class, FOREACH_LONG);
   }

   @Override
   public Set<Class<? extends BaseTerminalOperation>> getTypeClasses() {
      return Util.<Class<? extends BaseTerminalOperation>>asSet(SingleRunOperation.class,
              SegmentRetryingOperation.class, FlatMapIteratorOperation.class,
              MapIteratorOperation.class, NoMapIteratorOperation.class, ForEachOperation.class,
              ForEachFlatMapDoubleOperation.class, ForEachFlatMapIntOperation.class, ForEachFlatMapLongOperation.class,
              ForEachDoubleOperation.class, ForEachIntOperation.class, ForEachLongOperation.class);
   }

   @Override
   public Integer getId() {
      return Ids.TERMINAL_OPERATIONS;
   }

   @Override
   public void writeObject(ObjectOutput output, BaseTerminalOperation object) throws IOException {
      int number = operations.get(object.getClass(), -1);
      output.writeByte(number);
      output.writeObject(object.getIntermediateOperations());
      switch (number) {
         case SINGLE:
            output.writeObject(((SingleRunOperation) object).getFunction());
            break;
         case SEGMENT_RETRYING:
            output.writeObject(((SegmentRetryingOperation) object).getFunction());
            break;
         case FLATMAP_ITERATOR:
            UnsignedNumeric.writeUnsignedInt(output, ((FlatMapIteratorOperation) object).getBatchSize());
            break;
         case MAP_ITERATOR:
            UnsignedNumeric.writeUnsignedInt(output, ((MapIteratorOperation) object).getBatchSize());
            break;
         case NOMAP_ITERATOR:
            UnsignedNumeric.writeUnsignedInt(output, ((NoMapIteratorOperation) object).getBatchSize());
            break;
         case FOREACH:
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachOperation) object).getBatchSize());
            output.writeObject(((ForEachOperation) object).getConsumer());
            break;
         case FOREACH_FLAT_DOUBLE:
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachFlatMapDoubleOperation) object).getBatchSize());
            output.writeObject(((ForEachFlatMapDoubleOperation) object).getConsumer());
            break;
         case FOREACH_FLAT_INT:
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachFlatMapIntOperation) object).getBatchSize());
            output.writeObject(((ForEachFlatMapIntOperation) object).getConsumer());
            break;
         case FOREACH_FLAT_LONG:
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachFlatMapLongOperation) object).getBatchSize());
            output.writeObject(((ForEachFlatMapLongOperation) object).getConsumer());
            break;
         case FOREACH_DOUBLE:
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachDoubleOperation) object).getBatchSize());
            output.writeObject(((ForEachDoubleOperation) object).getConsumer());
            break;
         case FOREACH_INT:
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachIntOperation) object).getBatchSize());
            output.writeObject(((ForEachIntOperation) object).getConsumer());
            break;
         case FOREACH_LONG:
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachLongOperation) object).getBatchSize());
            output.writeObject(((ForEachLongOperation) object).getConsumer());
            break;
      }
   }

   @Override
   public BaseTerminalOperation readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int number = input.readUnsignedByte();
      switch (number) {
         case SINGLE:
            return new SingleRunOperation((Iterable<IntermediateOperation>) input.readObject(), null,
                    (Function) input.readObject());
         case SEGMENT_RETRYING:
            return new SegmentRetryingOperation((Iterable<IntermediateOperation>) input.readObject(), null,
                    (Function) input.readObject());
         case FLATMAP_ITERATOR:
            return new FlatMapIteratorOperation((Iterable<IntermediateOperation>) input.readObject(), null,
                    UnsignedNumeric.readUnsignedInt(input));
         case MAP_ITERATOR:
            return new MapIteratorOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                    UnsignedNumeric.readUnsignedInt(input));
         case NOMAP_ITERATOR:
            return new NoMapIteratorOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                    UnsignedNumeric.readUnsignedInt(input));
         case FOREACH:
            return new ForEachOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                    UnsignedNumeric.readUnsignedInt(input), (Consumer) input.readObject());
         case FOREACH_FLAT_DOUBLE:
            return new ForEachFlatMapDoubleOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                    UnsignedNumeric.readUnsignedInt(input), (DoubleConsumer) input.readObject());
         case FOREACH_FLAT_INT:
            return new ForEachFlatMapIntOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                    UnsignedNumeric.readUnsignedInt(input), (IntConsumer) input.readObject());
         case FOREACH_FLAT_LONG:
            return new ForEachFlatMapLongOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                    UnsignedNumeric.readUnsignedInt(input), (LongConsumer) input.readObject());
         case FOREACH_DOUBLE:
            return new ForEachDoubleOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                    UnsignedNumeric.readUnsignedInt(input), (DoubleConsumer) input.readObject());
         case FOREACH_INT:
            return new ForEachIntOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                    UnsignedNumeric.readUnsignedInt(input), (IntConsumer) input.readObject());
         case FOREACH_LONG:
            return new ForEachLongOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                    UnsignedNumeric.readUnsignedInt(input), (LongConsumer) input.readObject());

         default:
            throw new IllegalArgumentException("Found invalid number " + number);
      }
   }
}
