package org.infinispan.commons.marshall;

import static org.infinispan.commons.marshall.MarshallableFunctions.LambdaWithMetas;
import static org.infinispan.commons.marshall.MarshallableFunctions.SetValueIfEqualsReturnBoolean;
import static org.infinispan.commons.marshall.MarshallableFunctions.SetValueMetas;
import static org.infinispan.commons.marshall.MarshallableFunctions.SetValueMetasIfAbsentReturnBoolean;
import static org.infinispan.commons.marshall.MarshallableFunctions.SetValueMetasIfAbsentReturnPrevOrNull;
import static org.infinispan.commons.marshall.MarshallableFunctions.SetValueMetasIfPresentReturnBoolean;
import static org.infinispan.commons.marshall.MarshallableFunctions.SetValueMetasIfPresentReturnPrevOrNull;
import static org.infinispan.commons.marshall.MarshallableFunctions.SetValueMetasReturnPrevOrNull;
import static org.infinispan.commons.marshall.MarshallableFunctions.SetValueMetasReturnView;
import static org.infinispan.commons.marshall.MarshallableFunctions.identity;
import static org.infinispan.commons.marshall.MarshallableFunctions.removeConsumer;
import static org.infinispan.commons.marshall.MarshallableFunctions.removeIfValueEqualsReturnBoolean;
import static org.infinispan.commons.marshall.MarshallableFunctions.removeReturnBoolean;
import static org.infinispan.commons.marshall.MarshallableFunctions.removeReturnPrevOrNull;
import static org.infinispan.commons.marshall.MarshallableFunctions.returnReadOnlyFindIsPresent;
import static org.infinispan.commons.marshall.MarshallableFunctions.returnReadOnlyFindOrNull;
import static org.infinispan.commons.marshall.MarshallableFunctions.returnReadWriteFind;
import static org.infinispan.commons.marshall.MarshallableFunctions.returnReadWriteGet;
import static org.infinispan.commons.marshall.MarshallableFunctions.returnReadWriteView;
import static org.infinispan.commons.marshall.MarshallableFunctions.setValueConsumer;
import static org.infinispan.commons.marshall.MarshallableFunctions.setValueIfAbsentReturnBoolean;
import static org.infinispan.commons.marshall.MarshallableFunctions.setValueIfAbsentReturnPrevOrNull;
import static org.infinispan.commons.marshall.MarshallableFunctions.setValueIfPresentReturnBoolean;
import static org.infinispan.commons.marshall.MarshallableFunctions.setValueIfPresentReturnPrevOrNull;
import static org.infinispan.commons.marshall.MarshallableFunctions.setValueReturnPrevOrNull;
import static org.infinispan.commons.marshall.MarshallableFunctions.setValueReturnView;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.api.functional.MetaParam;
import org.infinispan.commons.util.Util;
import org.jboss.marshalling.util.IdentityIntMap;

public class MarshallableFunctionExternalizers {

   // TODO: Should really rely on ValuteMatcherMode enumeration ordering
   private static final short VALUE_MATCH_ALWAYS = 0x1000;
   private static final short VALUE_MATCH_EXPECTED = 0x2000;
   private static final short VALUE_MATCH_EXPECTED_OR_NEW = 0x3000;
   private static final short VALUE_MATCH_NON_NULL = 0x4000;
   private static final short VALUE_MATCH_NEVER = 0x5000;

   private static final int VALUE_MATCH_MASK = 0xF000;

   private static final int SET_VALUE_RETURN_PREV_OR_NULL = 1 | VALUE_MATCH_ALWAYS;
   private static final int SET_VALUE_RETURN_VIEW = 2 | VALUE_MATCH_ALWAYS;
   private static final int SET_VALUE_IF_ABSENT_RETURN_PREV_OR_NULL = 3 | VALUE_MATCH_EXPECTED;
   private static final int SET_VALUE_IF_ABSENT_RETURN_BOOLEAN = 4 | VALUE_MATCH_EXPECTED;
   private static final int SET_VALUE_IF_PRESENT_RETURN_PREV_OR_NULL = 5 | VALUE_MATCH_NON_NULL;
   private static final int SET_VALUE_IF_PRESENT_RETURN_BOOLEAN = 6  | VALUE_MATCH_NON_NULL;
   private static final int REMOVE_RETURN_PREV_OR_NULL = 7 | VALUE_MATCH_ALWAYS;
   private static final int REMOVE_RETURN_BOOLEAN = 8 | VALUE_MATCH_ALWAYS;
   private static final int REMOVE_IF_VALUE_EQUALS_RETURN_BOOLEAN = 9 | VALUE_MATCH_EXPECTED;
   private static final int SET_VALUE_CONSUMER = 10 | VALUE_MATCH_ALWAYS;
   private static final int REMOVE_CONSUMER = 11  | VALUE_MATCH_ALWAYS;
   private static final int RETURN_READ_WRITE_FIND = 12 | VALUE_MATCH_ALWAYS;
   private static final int RETURN_READ_WRITE_GET = 13 | VALUE_MATCH_ALWAYS;
   private static final int RETURN_READ_WRITE_VIEW = 14 | VALUE_MATCH_ALWAYS;
   private static final int RETURN_READ_ONLY_FIND_OR_NULL = 15 | VALUE_MATCH_ALWAYS;
   private static final int RETURN_READ_ONLY_FIND_IS_PRESENT = 16 | VALUE_MATCH_ALWAYS;
   private static final int IDENTITY = 17 | VALUE_MATCH_ALWAYS;

   public static final class ConstantLambdaExternalizer implements LambdaExternalizer<Object> {
      private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<>(16);

      public ConstantLambdaExternalizer() {
         numbers.put(setValueReturnPrevOrNull().getClass(), SET_VALUE_RETURN_PREV_OR_NULL);
         numbers.put(setValueReturnView().getClass(), SET_VALUE_RETURN_VIEW);
         numbers.put(setValueIfAbsentReturnPrevOrNull().getClass(), SET_VALUE_IF_ABSENT_RETURN_PREV_OR_NULL);
         numbers.put(setValueIfAbsentReturnBoolean().getClass(), SET_VALUE_IF_ABSENT_RETURN_BOOLEAN);
         numbers.put(setValueIfPresentReturnPrevOrNull().getClass(), SET_VALUE_IF_PRESENT_RETURN_PREV_OR_NULL);
         numbers.put(setValueIfPresentReturnBoolean().getClass(), SET_VALUE_IF_PRESENT_RETURN_BOOLEAN);
         numbers.put(removeReturnPrevOrNull().getClass(), REMOVE_RETURN_PREV_OR_NULL);
         numbers.put(removeReturnBoolean().getClass(), REMOVE_RETURN_BOOLEAN);
         numbers.put(removeIfValueEqualsReturnBoolean().getClass(), REMOVE_IF_VALUE_EQUALS_RETURN_BOOLEAN);
         numbers.put(setValueConsumer().getClass(), SET_VALUE_CONSUMER);
         numbers.put(removeConsumer().getClass(), REMOVE_CONSUMER);
         numbers.put(returnReadWriteFind().getClass(), RETURN_READ_WRITE_FIND);
         numbers.put(returnReadWriteGet().getClass(), RETURN_READ_WRITE_GET);
         numbers.put(returnReadWriteView().getClass(), RETURN_READ_WRITE_VIEW);
         numbers.put(returnReadOnlyFindOrNull().getClass(), RETURN_READ_ONLY_FIND_OR_NULL);
         numbers.put(returnReadOnlyFindIsPresent().getClass(), RETURN_READ_ONLY_FIND_IS_PRESENT);
         numbers.put(identity().getClass(), IDENTITY);
      }

      @Override
      public ValueMatcherMode valueMatcher(Object o) {
         int i = numbers.get(o.getClass(), -1);
         if (i > 0) {
            int valueMatcherId = ((i & VALUE_MATCH_MASK) >> 12) - 1;
            return ValueMatcherMode.valueOf(valueMatcherId);
         }

         return ValueMatcherMode.MATCH_ALWAYS;
      }

      @Override
      public Set<Class<?>> getTypeClasses() {
         return Util.<Class<?>>asSet(
            setValueReturnPrevOrNull().getClass(),
            setValueReturnView().getClass(),
            setValueIfAbsentReturnPrevOrNull().getClass(),
            setValueIfAbsentReturnBoolean().getClass(),
            setValueIfPresentReturnPrevOrNull().getClass(),
            setValueIfPresentReturnBoolean().getClass(),
            removeReturnPrevOrNull().getClass(),
            removeReturnBoolean().getClass(),
            removeIfValueEqualsReturnBoolean().getClass(),
            setValueConsumer().getClass(),
            removeConsumer().getClass(),
            returnReadWriteFind().getClass(),
            returnReadWriteGet().getClass(),
            returnReadWriteView().getClass(),
            returnReadOnlyFindOrNull().getClass(),
            returnReadOnlyFindIsPresent().getClass(),
            identity().getClass()
         );
      }

      @Override
      public Integer getId() {
         return Ids.LAMBDA_CONSTANT;
      }

      public void writeObject(ObjectOutput oo, Object o) throws IOException {
         int id = numbers.get(o.getClass(), -1);
         oo.writeShort(id);
      }

      public Object readObject(ObjectInput input) throws IOException {
         short id = input.readShort();
         switch (id) {
            case SET_VALUE_RETURN_PREV_OR_NULL: return setValueReturnPrevOrNull();
            case SET_VALUE_RETURN_VIEW: return setValueReturnView();
            case SET_VALUE_IF_ABSENT_RETURN_PREV_OR_NULL: return setValueIfAbsentReturnPrevOrNull();
            case SET_VALUE_IF_ABSENT_RETURN_BOOLEAN: return setValueIfAbsentReturnBoolean();
            case SET_VALUE_IF_PRESENT_RETURN_PREV_OR_NULL: return setValueIfPresentReturnPrevOrNull();
            case SET_VALUE_IF_PRESENT_RETURN_BOOLEAN: return setValueIfPresentReturnBoolean();
            case REMOVE_RETURN_PREV_OR_NULL: return removeReturnPrevOrNull();
            case REMOVE_RETURN_BOOLEAN: return removeReturnBoolean();
            case REMOVE_IF_VALUE_EQUALS_RETURN_BOOLEAN: return removeIfValueEqualsReturnBoolean();
            case SET_VALUE_CONSUMER: return setValueConsumer();
            case REMOVE_CONSUMER: return removeConsumer();
            case RETURN_READ_WRITE_FIND: return returnReadWriteFind();
            case RETURN_READ_WRITE_GET: return returnReadWriteGet();
            case RETURN_READ_WRITE_VIEW: return returnReadWriteView();
            case RETURN_READ_ONLY_FIND_OR_NULL: return returnReadOnlyFindOrNull();
            case RETURN_READ_ONLY_FIND_IS_PRESENT: return returnReadOnlyFindIsPresent();
            case IDENTITY: return identity();
            default:
               throw new IllegalStateException("Unknown lambda ID: " + id);
         }
      }
   }

   public static final class LambdaWithMetasExternalizer implements LambdaExternalizer<LambdaWithMetas> {
      private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<>(8);

      public LambdaWithMetasExternalizer() {
         numbers.put(SetValueMetasReturnPrevOrNull.class, SET_VALUE_RETURN_PREV_OR_NULL);
         numbers.put(SetValueMetasReturnView.class, SET_VALUE_RETURN_VIEW);
         numbers.put(SetValueMetasIfAbsentReturnPrevOrNull.class, SET_VALUE_IF_ABSENT_RETURN_PREV_OR_NULL);
         numbers.put(SetValueMetasIfAbsentReturnBoolean.class, SET_VALUE_IF_ABSENT_RETURN_BOOLEAN);
         numbers.put(SetValueMetasIfPresentReturnPrevOrNull.class, SET_VALUE_IF_PRESENT_RETURN_PREV_OR_NULL);
         numbers.put(SetValueMetasIfPresentReturnBoolean.class, SET_VALUE_IF_PRESENT_RETURN_BOOLEAN);
         numbers.put(SetValueMetas.class, SET_VALUE_CONSUMER);
      }

      @Override
      public ValueMatcherMode valueMatcher(Object o) {
         // TODO: Code duplication
         int i = numbers.get(o.getClass(), -1);
         if (i > 0) {
            int valueMatcherId = ((i & VALUE_MATCH_MASK) >> 12) - 1;
            return ValueMatcherMode.valueOf(valueMatcherId);
         }

         return ValueMatcherMode.MATCH_ALWAYS;
      }

      @Override
      public Set<Class<? extends LambdaWithMetas>> getTypeClasses() {
         return Util.<Class<? extends LambdaWithMetas>>asSet(
            SetValueMetasReturnPrevOrNull.class,
            SetValueMetasReturnView.class,
            SetValueMetasIfAbsentReturnPrevOrNull.class,
            SetValueMetasIfAbsentReturnBoolean.class,
            SetValueMetasIfPresentReturnPrevOrNull.class,
            SetValueMetasIfPresentReturnBoolean.class,
            SetValueMetas.class
         );
      }

      @Override
      public Integer getId() {
         return Ids.LAMBDA_WITH_METAS;
      }

      @Override
      public void writeObject(ObjectOutput oo, LambdaWithMetas o) throws IOException {
         int id = numbers.get(o.getClass(), -1);
         oo.writeShort(id);
         writeMetas(oo, o);
      }

      @Override
      public LambdaWithMetas readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         short id = input.readShort();
         MetaParam.Writable[] metas = readMetas(input);
         switch (id) {
            case SET_VALUE_RETURN_PREV_OR_NULL: return new SetValueMetasReturnPrevOrNull<>(metas);
            case SET_VALUE_IF_ABSENT_RETURN_PREV_OR_NULL: return new SetValueMetasIfAbsentReturnPrevOrNull<>(metas);
            case SET_VALUE_IF_ABSENT_RETURN_BOOLEAN: return new SetValueMetasIfAbsentReturnBoolean<>(metas);
            case SET_VALUE_RETURN_VIEW: return new SetValueMetasReturnView<>(metas);
            case SET_VALUE_IF_PRESENT_RETURN_PREV_OR_NULL: return new SetValueMetasIfPresentReturnPrevOrNull<>(metas);
            case SET_VALUE_IF_PRESENT_RETURN_BOOLEAN: return new SetValueMetasIfPresentReturnBoolean<>(metas);
            case SET_VALUE_CONSUMER: return new SetValueMetas<>(metas);
            default:
               throw new IllegalStateException("Unknown lambda and meta parameters with ID: " + id);
         }
      }
   }

   static MetaParam.Writable[] readMetas(ObjectInput input) throws IOException, ClassNotFoundException {
      int len = input.readInt();
      MetaParam.Writable[] metas = new MetaParam.Writable[len];
      for(int i = 0; i < len; i++)
         metas[i] = (MetaParam.Writable) input.readObject();
      return metas;
   }

   private static void writeMetas(ObjectOutput oo, LambdaWithMetas o) throws IOException {
      oo.writeInt(o.metas().length);
      for (MetaParam.Writable meta : o.metas())
         oo.writeObject(meta);
   }

   public static final class SetValueIfEqualsReturnBooleanExternalizer
            implements LambdaExternalizer<SetValueIfEqualsReturnBoolean> {
      public void writeObject(ObjectOutput oo, SetValueIfEqualsReturnBoolean o) throws IOException {
         oo.writeObject(o.oldValue);
         writeMetas(oo, o);
      }

      public SetValueIfEqualsReturnBoolean readObject(ObjectInput input)
         throws IOException, ClassNotFoundException {
         Object oldValue = input.readObject();
         MetaParam.Writable[] metas = readMetas(input);
         return new SetValueIfEqualsReturnBoolean<>(oldValue, metas);
      }

      @Override
      public ValueMatcherMode valueMatcher(Object o) {
         return ValueMatcherMode.MATCH_EXPECTED;
      }

      @Override
      public Set<Class<? extends SetValueIfEqualsReturnBoolean>> getTypeClasses() {
         return Util.<Class<? extends SetValueIfEqualsReturnBoolean>>asSet(SetValueIfEqualsReturnBoolean.class);
      }

      @Override
      public Integer getId() {
         return Ids.LAMBDA_SET_VALUE_IF_EQUALS_RETURN_BOOLEAN;
      }
   }

}
