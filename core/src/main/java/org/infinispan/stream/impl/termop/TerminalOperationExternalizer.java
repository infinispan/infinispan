package org.infinispan.stream.impl.termop;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.function.ObjDoubleConsumer;
import java.util.function.ObjIntConsumer;
import java.util.function.ObjLongConsumer;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.object.ForEachBiOperation;
import org.infinispan.stream.impl.termop.object.ForEachOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachDoubleOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachFlatMapDoubleOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachFlatMapIntOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachFlatMapLongOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachFlatMapObjDoubleOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachFlatMapObjIntOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachFlatMapObjLongOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachIntOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachLongOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachObjDoubleOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachObjIntOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachObjLongOperation;
import org.jboss.marshalling.util.IdentityIntMap;

/**
 * {@link AdvancedExternalizer} that provides functionality required for marshalling all of the various terminal
 * operations that are used by various distributed streams including the primitive forms.
 */
public class TerminalOperationExternalizer implements AdvancedExternalizer<BaseTerminalOperation> {
   private static final int SINGLE = 0;
   private static final int SEGMENT_RETRYING = 1;
   private static final int FOREACH = 2;
   private static final int FOREACH_FLAT_DOUBLE = 3;
   private static final int FOREACH_FLAT_INT = 4;
   private static final int FOREACH_FLAT_LONG = 5;
   private static final int FOREACH_DOUBLE = 6;
   private static final int FOREACH_INT = 7;
   private static final int FOREACH_LONG = 8;
   private static final int FOREACH_BI = 9;
   private static final int FOREACH_OBJ_DOUBLE = 10;
   private static final int FOREACH_OBJ_INT = 11;
   private static final int FOREACH_OBJ_LONG = 12;
   private static final int FOREACH_FLAT_OBJ_DOUBLE = 13;
   private static final int FOREACH_FLAT_OBJ_INT = 14;
   private static final int FOREACH_FLAT_OBJ_LONG = 15;

   private final IdentityIntMap<Class<? extends BaseTerminalOperation>> operations = new IdentityIntMap<>();

   public TerminalOperationExternalizer() {
      operations.put(SingleRunOperation.class, SINGLE);
      operations.put(SegmentRetryingOperation.class, SEGMENT_RETRYING);
      operations.put(ForEachOperation.class, FOREACH);
      operations.put(ForEachFlatMapDoubleOperation.class, FOREACH_FLAT_DOUBLE);
      operations.put(ForEachFlatMapIntOperation.class, FOREACH_FLAT_INT);
      operations.put(ForEachFlatMapLongOperation.class, FOREACH_FLAT_LONG);
      operations.put(ForEachDoubleOperation.class, FOREACH_DOUBLE);
      operations.put(ForEachIntOperation.class, FOREACH_INT);
      operations.put(ForEachLongOperation.class, FOREACH_LONG);
      operations.put(ForEachBiOperation.class, FOREACH_BI);
      operations.put(ForEachObjDoubleOperation.class, FOREACH_OBJ_DOUBLE);
      operations.put(ForEachObjIntOperation.class, FOREACH_OBJ_INT);
      operations.put(ForEachObjLongOperation.class, FOREACH_OBJ_LONG);
      operations.put(ForEachFlatMapObjDoubleOperation.class, FOREACH_FLAT_OBJ_DOUBLE);
      operations.put(ForEachFlatMapObjIntOperation.class, FOREACH_FLAT_OBJ_INT);
      operations.put(ForEachFlatMapObjLongOperation.class, FOREACH_FLAT_OBJ_LONG);
   }

   @Override
   public Set<Class<? extends BaseTerminalOperation>> getTypeClasses() {
      return Util.<Class<? extends BaseTerminalOperation>>asSet(SingleRunOperation.class,
              SegmentRetryingOperation.class, ForEachOperation.class,
              ForEachFlatMapDoubleOperation.class, ForEachFlatMapIntOperation.class, ForEachFlatMapLongOperation.class,
              ForEachDoubleOperation.class, ForEachIntOperation.class, ForEachLongOperation.class,
              ForEachBiOperation.class, ForEachObjDoubleOperation.class, ForEachObjIntOperation.class,
              ForEachObjLongOperation.class, ForEachFlatMapObjDoubleOperation.class,
              ForEachFlatMapObjIntOperation.class, ForEachFlatMapObjLongOperation.class);
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
         case FOREACH:
            output.writeObject(((ForEachOperation) object).getToKeyFunction());
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachOperation) object).getBatchSize());
            output.writeObject(((ForEachOperation) object).getConsumer());
            break;
         case FOREACH_FLAT_DOUBLE:
            output.writeObject(((ForEachFlatMapDoubleOperation) object).getToKeyFunction());
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachFlatMapDoubleOperation) object).getBatchSize());
            output.writeObject(((ForEachFlatMapDoubleOperation) object).getConsumer());
            break;
         case FOREACH_FLAT_INT:
            output.writeObject(((ForEachFlatMapIntOperation) object).getToKeyFunction());
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachFlatMapIntOperation) object).getBatchSize());
            output.writeObject(((ForEachFlatMapIntOperation) object).getConsumer());
            break;
         case FOREACH_FLAT_LONG:
            output.writeObject(((ForEachFlatMapLongOperation) object).getToKeyFunction());
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachFlatMapLongOperation) object).getBatchSize());
            output.writeObject(((ForEachFlatMapLongOperation) object).getConsumer());
            break;
         case FOREACH_DOUBLE:
            output.writeObject(((ForEachDoubleOperation) object).getToKeyFunction());
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachDoubleOperation) object).getBatchSize());
            output.writeObject(((ForEachDoubleOperation) object).getConsumer());
            break;
         case FOREACH_INT:
            output.writeObject(((ForEachIntOperation) object).getToKeyFunction());
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachIntOperation) object).getBatchSize());
            output.writeObject(((ForEachIntOperation) object).getConsumer());
            break;
         case FOREACH_LONG:
            output.writeObject(((ForEachLongOperation) object).getToKeyFunction());
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachLongOperation) object).getBatchSize());
            output.writeObject(((ForEachLongOperation) object).getConsumer());
            break;
         case FOREACH_BI:
            output.writeObject(((ForEachBiOperation) object).getToKeyFunction());
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachBiOperation) object).getBatchSize());
            output.writeObject(((ForEachBiOperation) object).getConsumer());
            break;
         case FOREACH_OBJ_DOUBLE:
            output.writeObject(((ForEachObjDoubleOperation) object).getToKeyFunction());
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachObjDoubleOperation) object).getBatchSize());
            output.writeObject(((ForEachObjDoubleOperation) object).getConsumer());
            break;
         case FOREACH_OBJ_INT:
            output.writeObject(((ForEachObjIntOperation) object).getToKeyFunction());
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachObjIntOperation) object).getBatchSize());
            output.writeObject(((ForEachObjIntOperation) object).getConsumer());
            break;
         case FOREACH_OBJ_LONG:
            output.writeObject(((ForEachObjLongOperation) object).getToKeyFunction());
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachObjLongOperation) object).getBatchSize());
            output.writeObject(((ForEachObjLongOperation) object).getConsumer());
            break;
         case FOREACH_FLAT_OBJ_DOUBLE:
            output.writeObject(((ForEachFlatMapObjDoubleOperation) object).getToKeyFunction());
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachFlatMapObjDoubleOperation) object).getBatchSize());
            output.writeObject(((ForEachFlatMapObjDoubleOperation) object).getConsumer());
            break;
         case FOREACH_FLAT_OBJ_INT:
            output.writeObject(((ForEachFlatMapObjIntOperation) object).getToKeyFunction());
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachFlatMapObjIntOperation) object).getBatchSize());
            output.writeObject(((ForEachFlatMapObjIntOperation) object).getConsumer());
            break;
         case FOREACH_FLAT_OBJ_LONG:
            output.writeObject(((ForEachFlatMapObjLongOperation) object).getToKeyFunction());
            UnsignedNumeric.writeUnsignedInt(output, ((ForEachFlatMapObjLongOperation) object).getBatchSize());
            output.writeObject(((ForEachFlatMapObjLongOperation) object).getConsumer());
            break;
         default:
            throw new IllegalArgumentException();
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
         case FOREACH:
            return new ForEachOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                  (Function) input.readObject(), UnsignedNumeric.readUnsignedInt(input), (Consumer) input.readObject());
         case FOREACH_FLAT_DOUBLE:
            return new ForEachFlatMapDoubleOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                  (Function) input.readObject(), UnsignedNumeric.readUnsignedInt(input),
                  (DoubleConsumer) input.readObject());
         case FOREACH_FLAT_INT:
            return new ForEachFlatMapIntOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                  (Function) input.readObject(), UnsignedNumeric.readUnsignedInt(input),
                  (IntConsumer) input.readObject());
         case FOREACH_FLAT_LONG:
            return new ForEachFlatMapLongOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                  (Function) input.readObject(), UnsignedNumeric.readUnsignedInt(input),
                  (LongConsumer) input.readObject());
         case FOREACH_DOUBLE:
            return new ForEachDoubleOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                  (Function) input.readObject(), UnsignedNumeric.readUnsignedInt(input),
                  (DoubleConsumer) input.readObject());
         case FOREACH_INT:
            return new ForEachIntOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                  (Function) input.readObject(), UnsignedNumeric.readUnsignedInt(input),
                  (IntConsumer) input.readObject());
         case FOREACH_LONG:
            return new ForEachLongOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                  (Function) input.readObject(), UnsignedNumeric.readUnsignedInt(input),
                  (LongConsumer) input.readObject());
         case FOREACH_BI:
            return new ForEachBiOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                  (Function) input.readObject(), UnsignedNumeric.readUnsignedInt(input),
                  (BiConsumer) input.readObject());
         case FOREACH_OBJ_DOUBLE:
            return new ForEachObjDoubleOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                  (Function) input.readObject(),   UnsignedNumeric.readUnsignedInt(input),
                  (ObjDoubleConsumer) input.readObject());
         case FOREACH_OBJ_INT:
            return new ForEachObjIntOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                  (Function) input.readObject(), UnsignedNumeric.readUnsignedInt(input),
                  (ObjIntConsumer) input.readObject());
         case FOREACH_OBJ_LONG:
            return new ForEachObjLongOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                  (Function) input.readObject(), UnsignedNumeric.readUnsignedInt(input),
                  (ObjLongConsumer) input.readObject());
         case FOREACH_FLAT_OBJ_DOUBLE:
            return new ForEachFlatMapObjDoubleOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                  (Function) input.readObject(), UnsignedNumeric.readUnsignedInt(input),
                  (ObjDoubleConsumer) input.readObject());
         case FOREACH_FLAT_OBJ_INT:
            return new ForEachFlatMapObjIntOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                  (Function) input.readObject(), UnsignedNumeric.readUnsignedInt(input),
                  (ObjIntConsumer) input.readObject());
         case FOREACH_FLAT_OBJ_LONG:
            return new ForEachFlatMapObjLongOperation<>((Iterable<IntermediateOperation>) input.readObject(), null,
                  (Function) input.readObject(), UnsignedNumeric.readUnsignedInt(input),
                  (ObjLongConsumer) input.readObject());

         default:
            throw new IllegalArgumentException("Found invalid number " + number);
      }
   }
}
