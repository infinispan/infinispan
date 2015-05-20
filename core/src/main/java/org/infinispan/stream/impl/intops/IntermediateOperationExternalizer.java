package org.infinispan.stream.impl.intops;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.stream.impl.intops.object.*;
import org.infinispan.stream.impl.intops.primitive.d.*;
import org.infinispan.stream.impl.intops.primitive.i.*;
import org.infinispan.stream.impl.intops.primitive.l.*;
import org.jboss.marshalling.util.IdentityIntMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Comparator;
import java.util.Set;
import java.util.function.*;

/**
 * Externalizer to be used for serializing the various intermediate operations
 */
public class IntermediateOperationExternalizer implements AdvancedExternalizer<IntermediateOperation> {
   // Object stream intermediate operations
   private static final int DISTINCT = 0;
   private static final int FILTER = 1;
   private static final int FLATMAP = 2;
   private static final int FLATMAP_DOUBLE = 3;
   private static final int FLATMAP_INT = 4;
   private static final int FLATMAP_LONG = 5;
   private static final int LIMIT = 6;
   private static final int MAP = 7;
   private static final int MAP_DOUBLE = 8;
   private static final int MAP_INT = 9;
   private static final int MAP_LONG = 10;
   private static final int PEEK = 11;
   private static final int SORTED_COMPARATOR = 12;
   private static final int SORTED = 13;

   // Double stream intermediate operations
   private static final int DOUBLE_BOXED = 20;
   private static final int DOUBLE_DISTINCT = 21;
   private static final int DOUBLE_FILTER = 22;
   private static final int DOUBLE_FLATMAP = 23;
   private static final int DOUBLE_LIMIT = 24;
   private static final int DOUBLE_MAP = 25;
   private static final int DOUBLE_MAP_INT = 26;
   private static final int DOUBLE_MAP_LONG = 27;
   private static final int DOUBLE_MAP_OBJ = 28;
   private static final int DOUBLE_PEEK = 29;
   private static final int DOUBLE_SORTED = 30;

   // Int stream intermediate operations
   private static final int INT_AS_DOUBLE = 40;
   private static final int INT_AS_LONG = 41;
   private static final int INT_BOXED = 42;
   private static final int INT_DISTINCT = 43;
   private static final int INT_FILTER = 44;
   private static final int INT_FLATMAP = 45;
   private static final int INT_LIMIT = 46;
   private static final int INT_MAP = 47;
   private static final int INT_MAP_DOUBLE = 48;
   private static final int INT_MAP_LONG = 49;
   private static final int INT_MAP_OBJ = 50;
   private static final int INT_PEEK = 51;
   private static final int INT_SORTED = 52;

   // Long stream intermediate operations
   private static final int LONG_AS_DOUBLE = 60;
   private static final int LONG_BOXED = 61;
   private static final int LONG_DISTINCT = 62;
   private static final int LONG_FILTER = 63;
   private static final int LONG_FLATMAP = 64;
   private static final int LONG_LIMIT = 65;
   private static final int LONG_MAP = 66;
   private static final int LONG_MAP_DOUBLE = 67;
   private static final int LONG_MAP_INT = 68;
   private static final int LONG_MAP_OBJ = 69;
   private static final int LONG_PEEK = 70;
   private static final int LONG_SORTED = 71;

   private final IdentityIntMap<Class<? extends IntermediateOperation>> operations = new IdentityIntMap<>();

   public IntermediateOperationExternalizer() {
      operations.put(DistinctOperation.class, DISTINCT);
      operations.put(FilterOperation.class, FILTER);
      operations.put(FlatMapOperation.class, FLATMAP);
      operations.put(FlatMapToDoubleOperation.class, FLATMAP_DOUBLE);
      operations.put(FlatMapToIntOperation.class, FLATMAP_INT);
      operations.put(FlatMapToLongOperation.class, FLATMAP_LONG);
      operations.put(LimitOperation.class, LIMIT);
      operations.put(MapOperation.class, MAP);
      operations.put(MapToDoubleOperation.class, MAP_DOUBLE);
      operations.put(MapToIntOperation.class, MAP_INT);
      operations.put(MapToLongOperation.class, MAP_LONG);
      operations.put(PeekOperation.class, PEEK);
      operations.put(SortedComparatorOperation.class, SORTED_COMPARATOR);
      operations.put(SortedOperation.class, SORTED);

      operations.put(BoxedDoubleOperation.class, DOUBLE_BOXED);
      operations.put(DistinctDoubleOperation.class, DOUBLE_DISTINCT);
      operations.put(FilterDoubleOperation.class, DOUBLE_FILTER);
      operations.put(FlatMapDoubleOperation.class, DOUBLE_FLATMAP);
      operations.put(LimitDoubleOperation.class, DOUBLE_LIMIT);
      operations.put(MapDoubleOperation.class, DOUBLE_MAP);
      operations.put(MapToIntDoubleOperation.class, DOUBLE_MAP_INT);
      operations.put(MapToLongDoubleOperation.class, DOUBLE_MAP_LONG);
      operations.put(MapToObjDoubleOperation.class, DOUBLE_MAP_OBJ);
      operations.put(PeekDoubleOperation.class, DOUBLE_PEEK);
      operations.put(SortedDoubleOperation.class, DOUBLE_SORTED);

      operations.put(AsDoubleIntOperation.class, INT_AS_DOUBLE);
      operations.put(AsLongIntOperation.class, INT_AS_LONG);
      operations.put(BoxedIntOperation.class, INT_BOXED);
      operations.put(DistinctIntOperation.class, INT_DISTINCT);
      operations.put(FilterIntOperation.class, INT_FILTER);
      operations.put(FlatMapIntOperation.class, INT_FLATMAP);
      operations.put(LimitIntOperation.class, INT_LIMIT);
      operations.put(MapIntOperation.class, INT_MAP);
      operations.put(MapToDoubleIntOperation.class, INT_MAP_DOUBLE);
      operations.put(MapToLongIntOperation.class, INT_MAP_LONG);
      operations.put(MapToObjIntOperation.class, INT_MAP_OBJ);
      operations.put(PeekIntOperation.class, INT_PEEK);
      operations.put(SortedIntOperation.class, INT_SORTED);

      operations.put(AsDoubleLongOperation.class, LONG_AS_DOUBLE);
      operations.put(BoxedLongOperation.class, LONG_BOXED);
      operations.put(DistinctLongOperation.class, LONG_DISTINCT);
      operations.put(FilterLongOperation.class, LONG_FILTER);
      operations.put(FlatMapLongOperation.class, LONG_FLATMAP);
      operations.put(LimitLongOperation.class, LONG_LIMIT);
      operations.put(MapLongOperation.class, LONG_MAP);
      operations.put(MapToDoubleLongOperation.class, LONG_MAP_DOUBLE);
      operations.put(MapToIntLongOperation.class, LONG_MAP_INT);
      operations.put(MapToObjLongOperation.class, LONG_MAP_OBJ);
      operations.put(PeekLongOperation.class, LONG_PEEK);
      operations.put(SortedLongOperation.class, LONG_SORTED);
   }

   @Override
   public Set<Class<? extends IntermediateOperation>> getTypeClasses() {
      return Util.<Class<? extends IntermediateOperation>>asSet(DistinctOperation.class, FilterOperation.class,
              FlatMapOperation.class, FlatMapToDoubleOperation.class, FlatMapToIntOperation.class,
              FlatMapToLongOperation.class, LimitOperation.class, MapOperation.class, MapToDoubleOperation.class,
              MapToIntOperation.class, MapToLongOperation.class, PeekOperation.class,
              SortedComparatorOperation.class, SortedOperation.class,

              BoxedDoubleOperation.class, DistinctDoubleOperation.class, FilterDoubleOperation.class,
              FlatMapDoubleOperation.class, LimitDoubleOperation.class, MapDoubleOperation.class,
              MapToIntDoubleOperation.class, MapToLongDoubleOperation.class, MapToDoubleOperation.class,
              PeekDoubleOperation.class, SortedDoubleOperation.class,

              AsDoubleIntOperation.class, AsLongIntOperation.class, BoxedIntOperation.class, DistinctOperation.class,
              FilterIntOperation.class, FlatMapIntOperation.class, LimitIntOperation.class,
              MapIntOperation.class, MapToDoubleIntOperation.class, MapToLongIntOperation.class,
              MapToObjIntOperation.class, PeekIntOperation.class, SortedIntOperation.class,

              AsDoubleLongOperation.class, BoxedLongOperation.class, DistinctOperation.class, FilterLongOperation.class,
              FlatMapLongOperation.class, LimitOperation.class, MapLongOperation.class, MapToDoubleLongOperation.class,
              MapToIntLongOperation.class, MapToObjLongOperation.class, PeekLongOperation.class,
              SortedLongOperation.class
      );
   }

   @Override
   public Integer getId() {
      return Ids.INTERMEDIATE_OPERATIONS;
   }

   @Override
   public void writeObject(ObjectOutput output, IntermediateOperation object) throws IOException {
      int number = operations.get(object.getClass(), -1);
      output.writeByte(number);
      switch (number) {
         case FILTER:
            output.writeObject(((FilterOperation) object).getPredicate());
            break;
         case FLATMAP:
            output.writeObject(((FlatMapOperation) object).getFunction());
            break;
         case FLATMAP_DOUBLE:
            output.writeObject(((FlatMapToDoubleOperation) object).getFunction());
            break;
         case FLATMAP_INT:
            output.writeObject(((FlatMapToIntOperation) object).getFunction());
            break;
         case FLATMAP_LONG:
            output.writeObject(((FlatMapToLongOperation) object).getFunction());
            break;
         case LIMIT:
            UnsignedNumeric.writeUnsignedLong(output, ((LimitOperation) object).getLimit());
            break;
         case MAP:
            output.writeObject(((MapOperation) object).getFunction());
            break;
         case MAP_DOUBLE:
            output.writeObject(((MapToDoubleOperation) object).getFunction());
            break;
         case MAP_INT:
            output.writeObject(((MapToIntOperation) object).getFunction());
            break;
         case MAP_LONG:
            output.writeObject(((MapToLongOperation) object).getFunction());
            break;
         case PEEK:
            output.writeObject(((PeekOperation) object).getConsumer());
            break;
         case SORTED_COMPARATOR:
            output.writeObject(((SortedComparatorOperation) object).getComparator());
            break;

         case DOUBLE_FILTER:
            output.writeObject(((FilterDoubleOperation) object).getPredicate());
            break;
         case DOUBLE_FLATMAP:
            output.writeObject(((FlatMapDoubleOperation) object).getFunction());
            break;
         case DOUBLE_LIMIT:
            UnsignedNumeric.writeUnsignedLong(output, ((LimitDoubleOperation) object).getLimit());
            break;
         case DOUBLE_MAP:
            output.writeObject(((MapDoubleOperation) object).getOperator());
            break;
         case DOUBLE_MAP_INT:
            output.writeObject(((MapToIntDoubleOperation) object).getFunction());
            break;
         case DOUBLE_MAP_LONG:
            output.writeObject(((MapToLongDoubleOperation) object).getFunction());
            break;
         case DOUBLE_MAP_OBJ:
            output.writeObject(((MapToObjDoubleOperation) object).getFunction());
            break;
         case DOUBLE_PEEK:
            output.writeObject(((PeekDoubleOperation) object).getConsumer());
            break;

         case INT_FILTER:
            output.writeObject(((FilterIntOperation) object).getPredicate());
            break;
         case INT_FLATMAP:
            output.writeObject(((FlatMapIntOperation) object).getFunction());
            break;
         case INT_LIMIT:
            UnsignedNumeric.writeUnsignedLong(output, ((LimitIntOperation) object).getLimit());
            break;
         case INT_MAP:
            output.writeObject(((MapIntOperation) object).getOperator());
            break;
         case INT_MAP_DOUBLE:
            output.writeObject(((MapToDoubleIntOperation) object).getFunction());
            break;
         case INT_MAP_LONG:
            output.writeObject(((MapToLongIntOperation) object).getFunction());
            break;
         case INT_MAP_OBJ:
            output.writeObject(((MapToObjIntOperation) object).getFunction());
            break;
         case INT_PEEK:
            output.writeObject(((PeekIntOperation) object).getConsumer());
            break;

         case LONG_FILTER:
            output.writeObject(((FilterLongOperation) object).getPredicate());
            break;
         case LONG_FLATMAP:
            output.writeObject(((FlatMapLongOperation) object).getFunction());
            break;
         case LONG_LIMIT:
            UnsignedNumeric.writeUnsignedLong(output, ((LimitLongOperation) object).getLimit());
            break;
         case LONG_MAP:
            output.writeObject(((MapLongOperation) object).getOperator());
            break;
         case LONG_MAP_DOUBLE:
            output.writeObject(((MapToDoubleLongOperation) object).getFunction());
            break;
         case LONG_MAP_INT:
            output.writeObject(((MapToIntLongOperation) object).getFunction());
            break;
         case LONG_MAP_OBJ:
            output.writeObject(((MapToObjLongOperation) object).getFunction());
            break;
         case LONG_PEEK:
            output.writeObject(((PeekLongOperation) object).getConsumer());
            break;
      }
   }

   @Override
   public IntermediateOperation readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int number = input.readUnsignedByte();
      switch (number) {
         case DISTINCT:
            return DistinctOperation.getInstance();
         case FILTER:
            return new FilterOperation<>((Predicate) input.readObject());
         case FLATMAP:
            return new FlatMapOperation<>((Function) input.readObject());
         case FLATMAP_DOUBLE:
            return new FlatMapToDoubleOperation((Function) input.readObject());
         case FLATMAP_INT:
            return new FlatMapToIntOperation((Function) input.readObject());
         case FLATMAP_LONG:
            return new FlatMapToLongOperation<>((Function) input.readObject());
         case LIMIT:
            return new LimitOperation<>(UnsignedNumeric.readUnsignedLong(input));
         case MAP:
            return new MapOperation<>((Function) input.readObject());
         case MAP_DOUBLE:
            return new MapToDoubleOperation<>((ToDoubleFunction) input.readObject());
         case MAP_INT:
            return new MapToIntOperation<>((ToIntFunction) input.readObject());
         case MAP_LONG:
            return new MapToLongOperation<>((ToLongFunction) input.readObject());
         case PEEK:
            return new PeekOperation<>((Consumer) input.readObject());
         case SORTED_COMPARATOR:
            return new SortedComparatorOperation<>((Comparator) input.readObject());
         case SORTED:
            return SortedOperation.getInstance();

         case DOUBLE_BOXED:
            return BoxedDoubleOperation.getInstance();
         case DOUBLE_DISTINCT:
            return DistinctDoubleOperation.getInstance();
         case DOUBLE_FILTER:
            return new FilterDoubleOperation((DoublePredicate) input.readObject());
         case DOUBLE_FLATMAP:
            return new FlatMapDoubleOperation((DoubleFunction) input.readObject());
         case DOUBLE_LIMIT:
            return new LimitDoubleOperation(UnsignedNumeric.readUnsignedLong(input));
         case DOUBLE_MAP:
            return new MapDoubleOperation((DoubleUnaryOperator) input.readObject());
         case DOUBLE_MAP_INT:
            return new MapToIntDoubleOperation((DoubleToIntFunction) input.readObject());
         case DOUBLE_MAP_LONG:
            return new MapToLongDoubleOperation((DoubleToLongFunction) input.readObject());
         case DOUBLE_MAP_OBJ:
            return new MapToObjDoubleOperation<>((DoubleFunction) input.readObject());
         case DOUBLE_PEEK:
            return new PeekDoubleOperation((DoubleConsumer) input.readObject());
         case DOUBLE_SORTED:
            return SortedDoubleOperation.getInstance();

         case INT_AS_DOUBLE:
            return AsDoubleIntOperation.getInstance();
         case INT_AS_LONG:
            return AsLongIntOperation.getInstance();
         case INT_BOXED:
            return BoxedIntOperation.getInstance();
         case INT_DISTINCT:
            return DistinctIntOperation.getInstance();
         case INT_FILTER:
            return new FilterIntOperation((IntPredicate) input.readObject());
         case INT_FLATMAP:
            return new FlatMapIntOperation((IntFunction) input.readObject());
         case INT_LIMIT:
            return new LimitIntOperation(UnsignedNumeric.readUnsignedLong(input));
         case INT_MAP:
            return new MapIntOperation((IntUnaryOperator) input.readObject());
         case INT_MAP_DOUBLE:
            return new MapToDoubleIntOperation((IntToDoubleFunction) input.readObject());
         case INT_MAP_LONG:
            return new MapToLongIntOperation((IntToLongFunction) input.readObject());
         case INT_MAP_OBJ:
            return new MapToObjIntOperation<>((IntFunction) input.readObject());
         case INT_PEEK:
            return new PeekIntOperation((IntConsumer) input.readObject());
         case INT_SORTED:
            return SortedIntOperation.getInstance();

         case LONG_AS_DOUBLE:
            return AsDoubleLongOperation.getInstance();
         case LONG_BOXED:
            return BoxedLongOperation.getInstance();
         case LONG_DISTINCT:
            return DistinctLongOperation.getInstance();
         case LONG_FILTER:
            return new FilterLongOperation((LongPredicate) input.readObject());
         case LONG_FLATMAP:
            return new FlatMapLongOperation((LongFunction) input.readObject());
         case LONG_LIMIT:
            return new LimitLongOperation(UnsignedNumeric.readUnsignedLong(input));
         case LONG_MAP:
            return new MapLongOperation((LongUnaryOperator) input.readObject());
         case LONG_MAP_DOUBLE:
            return new MapToDoubleLongOperation((LongToDoubleFunction) input.readObject());
         case LONG_MAP_INT:
            return new MapToIntLongOperation((LongToIntFunction) input.readObject());
         case LONG_MAP_OBJ:
            return new MapToObjLongOperation<>((LongFunction) input.readObject());
         case LONG_PEEK:
            return new PeekLongOperation((LongConsumer) input.readObject());
         case LONG_SORTED:
            return SortedLongOperation.getInstance();

         default:
            throw new IllegalArgumentException("Found invalid number " + number);
      }
   }
}
