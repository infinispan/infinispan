package org.infinispan.marshall.core;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.marshall.LambdaExternalizer;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.marshall.ValueMatcherMode;
import org.infinispan.commons.util.Util;
import org.infinispan.functional.MetaParam;
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
   private static final int SET_INTERNAL_CACHE_VALUE_CONSUMER = 18 | VALUE_MATCH_ALWAYS;

   public static final class ConstantLambdaExternalizer implements LambdaExternalizer<Object> {
      private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<>(16);

      public ConstantLambdaExternalizer() {
         numbers.put(MarshallableFunctions.setValueReturnPrevOrNull().getClass(), SET_VALUE_RETURN_PREV_OR_NULL);
         numbers.put(MarshallableFunctions.setValueReturnView().getClass(), SET_VALUE_RETURN_VIEW);
         numbers.put(MarshallableFunctions.setValueIfAbsentReturnPrevOrNull().getClass(), SET_VALUE_IF_ABSENT_RETURN_PREV_OR_NULL);
         numbers.put(MarshallableFunctions.setValueIfAbsentReturnBoolean().getClass(), SET_VALUE_IF_ABSENT_RETURN_BOOLEAN);
         numbers.put(MarshallableFunctions.setValueIfPresentReturnPrevOrNull().getClass(), SET_VALUE_IF_PRESENT_RETURN_PREV_OR_NULL);
         numbers.put(MarshallableFunctions.setValueIfPresentReturnBoolean().getClass(), SET_VALUE_IF_PRESENT_RETURN_BOOLEAN);
         numbers.put(MarshallableFunctions.removeReturnPrevOrNull().getClass(), REMOVE_RETURN_PREV_OR_NULL);
         numbers.put(MarshallableFunctions.removeReturnBoolean().getClass(), REMOVE_RETURN_BOOLEAN);
         numbers.put(MarshallableFunctions.removeIfValueEqualsReturnBoolean().getClass(), REMOVE_IF_VALUE_EQUALS_RETURN_BOOLEAN);
         numbers.put(MarshallableFunctions.setValueConsumer().getClass(), SET_VALUE_CONSUMER);
         numbers.put(MarshallableFunctions.removeConsumer().getClass(), REMOVE_CONSUMER);
         numbers.put(MarshallableFunctions.returnReadWriteFind().getClass(), RETURN_READ_WRITE_FIND);
         numbers.put(MarshallableFunctions.returnReadWriteGet().getClass(), RETURN_READ_WRITE_GET);
         numbers.put(MarshallableFunctions.returnReadWriteView().getClass(), RETURN_READ_WRITE_VIEW);
         numbers.put(MarshallableFunctions.returnReadOnlyFindOrNull().getClass(), RETURN_READ_ONLY_FIND_OR_NULL);
         numbers.put(MarshallableFunctions.returnReadOnlyFindIsPresent().getClass(), RETURN_READ_ONLY_FIND_IS_PRESENT);
         numbers.put(MarshallableFunctions.identity().getClass(), IDENTITY);
         numbers.put(MarshallableFunctions.setInternalCacheValueConsumer().getClass(), SET_INTERNAL_CACHE_VALUE_CONSUMER);
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
            MarshallableFunctions.setValueReturnPrevOrNull().getClass(),
            MarshallableFunctions.setValueReturnView().getClass(),
            MarshallableFunctions.setValueIfAbsentReturnPrevOrNull().getClass(),
            MarshallableFunctions.setValueIfAbsentReturnBoolean().getClass(),
            MarshallableFunctions.setValueIfPresentReturnPrevOrNull().getClass(),
            MarshallableFunctions.setValueIfPresentReturnBoolean().getClass(),
            MarshallableFunctions.removeReturnPrevOrNull().getClass(),
            MarshallableFunctions.removeReturnBoolean().getClass(),
            MarshallableFunctions.removeIfValueEqualsReturnBoolean().getClass(),
            MarshallableFunctions.setValueConsumer().getClass(),
            MarshallableFunctions.removeConsumer().getClass(),
            MarshallableFunctions.returnReadWriteFind().getClass(),
            MarshallableFunctions.returnReadWriteGet().getClass(),
            MarshallableFunctions.returnReadWriteView().getClass(),
            MarshallableFunctions.returnReadOnlyFindOrNull().getClass(),
            MarshallableFunctions.returnReadOnlyFindIsPresent().getClass(),
            MarshallableFunctions.identity().getClass(),
            MarshallableFunctions.setInternalCacheValueConsumer().getClass()
         );
      }

      @Override
      public Integer getId() {
         return org.infinispan.commons.marshall.Ids.LAMBDA_CONSTANT;
      }

      public void writeObject(UserObjectOutput oo, Object o) throws IOException {
         int id = numbers.get(o.getClass(), -1);
         oo.writeShort(id);
      }

      public Object readObject(UserObjectInput input) throws IOException {
         short id = input.readShort();
         switch (id) {
            case SET_VALUE_RETURN_PREV_OR_NULL: return MarshallableFunctions.setValueReturnPrevOrNull();
            case SET_VALUE_RETURN_VIEW: return MarshallableFunctions.setValueReturnView();
            case SET_VALUE_IF_ABSENT_RETURN_PREV_OR_NULL: return MarshallableFunctions.setValueIfAbsentReturnPrevOrNull();
            case SET_VALUE_IF_ABSENT_RETURN_BOOLEAN: return MarshallableFunctions.setValueIfAbsentReturnBoolean();
            case SET_VALUE_IF_PRESENT_RETURN_PREV_OR_NULL: return MarshallableFunctions.setValueIfPresentReturnPrevOrNull();
            case SET_VALUE_IF_PRESENT_RETURN_BOOLEAN: return MarshallableFunctions.setValueIfPresentReturnBoolean();
            case REMOVE_RETURN_PREV_OR_NULL: return MarshallableFunctions.removeReturnPrevOrNull();
            case REMOVE_RETURN_BOOLEAN: return MarshallableFunctions.removeReturnBoolean();
            case REMOVE_IF_VALUE_EQUALS_RETURN_BOOLEAN: return MarshallableFunctions.removeIfValueEqualsReturnBoolean();
            case SET_VALUE_CONSUMER: return MarshallableFunctions.setValueConsumer();
            case REMOVE_CONSUMER: return MarshallableFunctions.removeConsumer();
            case RETURN_READ_WRITE_FIND: return MarshallableFunctions.returnReadWriteFind();
            case RETURN_READ_WRITE_GET: return MarshallableFunctions.returnReadWriteGet();
            case RETURN_READ_WRITE_VIEW: return MarshallableFunctions.returnReadWriteView();
            case RETURN_READ_ONLY_FIND_OR_NULL: return MarshallableFunctions.returnReadOnlyFindOrNull();
            case RETURN_READ_ONLY_FIND_IS_PRESENT: return MarshallableFunctions.returnReadOnlyFindIsPresent();
            case IDENTITY: return MarshallableFunctions.identity();
            case SET_INTERNAL_CACHE_VALUE_CONSUMER: return MarshallableFunctions.setInternalCacheValueConsumer();
            default:
               throw new IllegalStateException("Unknown lambda ID: " + id);
         }
      }
   }

   public static final class LambdaWithMetasExternalizer implements LambdaExternalizer<MarshallableFunctions.LambdaWithMetas> {
      private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<>(8);

      public LambdaWithMetasExternalizer() {
         numbers.put(MarshallableFunctions.SetValueMetasReturnPrevOrNull.class, SET_VALUE_RETURN_PREV_OR_NULL);
         numbers.put(MarshallableFunctions.SetValueMetasReturnView.class, SET_VALUE_RETURN_VIEW);
         numbers.put(MarshallableFunctions.SetValueMetasIfAbsentReturnPrevOrNull.class, SET_VALUE_IF_ABSENT_RETURN_PREV_OR_NULL);
         numbers.put(MarshallableFunctions.SetValueMetasIfAbsentReturnBoolean.class, SET_VALUE_IF_ABSENT_RETURN_BOOLEAN);
         numbers.put(MarshallableFunctions.SetValueMetasIfPresentReturnPrevOrNull.class, SET_VALUE_IF_PRESENT_RETURN_PREV_OR_NULL);
         numbers.put(MarshallableFunctions.SetValueMetasIfPresentReturnBoolean.class, SET_VALUE_IF_PRESENT_RETURN_BOOLEAN);
         numbers.put(MarshallableFunctions.SetValueMetas.class, SET_VALUE_CONSUMER);
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
      public Set<Class<? extends MarshallableFunctions.LambdaWithMetas>> getTypeClasses() {
         return Util.<Class<? extends MarshallableFunctions.LambdaWithMetas>>asSet(
            MarshallableFunctions.SetValueMetasReturnPrevOrNull.class,
            MarshallableFunctions.SetValueMetasReturnView.class,
            MarshallableFunctions.SetValueMetasIfAbsentReturnPrevOrNull.class,
            MarshallableFunctions.SetValueMetasIfAbsentReturnBoolean.class,
            MarshallableFunctions.SetValueMetasIfPresentReturnPrevOrNull.class,
            MarshallableFunctions.SetValueMetasIfPresentReturnBoolean.class,
            MarshallableFunctions.SetValueMetas.class
         );
      }

      @Override
      public Integer getId() {
         return org.infinispan.commons.marshall.Ids.LAMBDA_WITH_METAS;
      }

      @Override
      public void writeObject(UserObjectOutput oo, MarshallableFunctions.LambdaWithMetas o) throws IOException {
         int id = numbers.get(o.getClass(), -1);
         oo.writeShort(id);
         writeMetas(oo, o);
      }

      @Override
      public MarshallableFunctions.LambdaWithMetas readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         short id = input.readShort();
         MetaParam.Writable[] metas = readMetas(input);
         switch (id) {
            case SET_VALUE_RETURN_PREV_OR_NULL: return new MarshallableFunctions.SetValueMetasReturnPrevOrNull<>(metas);
            case SET_VALUE_IF_ABSENT_RETURN_PREV_OR_NULL: return new MarshallableFunctions.SetValueMetasIfAbsentReturnPrevOrNull<>(metas);
            case SET_VALUE_IF_ABSENT_RETURN_BOOLEAN: return new MarshallableFunctions.SetValueMetasIfAbsentReturnBoolean<>(metas);
            case SET_VALUE_RETURN_VIEW: return new MarshallableFunctions.SetValueMetasReturnView<>(metas);
            case SET_VALUE_IF_PRESENT_RETURN_PREV_OR_NULL: return new MarshallableFunctions.SetValueMetasIfPresentReturnPrevOrNull<>(metas);
            case SET_VALUE_IF_PRESENT_RETURN_BOOLEAN: return new MarshallableFunctions.SetValueMetasIfPresentReturnBoolean<>(metas);
            case SET_VALUE_CONSUMER: return new MarshallableFunctions.SetValueMetas<>(metas);
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

   private static void writeMetas(ObjectOutput oo, MarshallableFunctions.LambdaWithMetas o) throws IOException {
      oo.writeInt(o.metas().length);
      for (MetaParam.Writable meta : o.metas())
         oo.writeObject(meta);
   }

   public static final class SetValueIfEqualsReturnBooleanExternalizer
            implements LambdaExternalizer<MarshallableFunctions.SetValueIfEqualsReturnBoolean> {
      public void writeObject(UserObjectOutput oo, MarshallableFunctions.SetValueIfEqualsReturnBoolean o) throws IOException {
         oo.writeObject(o.oldValue);
         writeMetas(oo, o);
      }

      public MarshallableFunctions.SetValueIfEqualsReturnBoolean readObject(UserObjectInput input)
         throws IOException, ClassNotFoundException {
         Object oldValue = input.readObject();
         MetaParam.Writable[] metas = readMetas(input);
         return new MarshallableFunctions.SetValueIfEqualsReturnBoolean<>(oldValue, metas);
      }

      @Override
      public ValueMatcherMode valueMatcher(Object o) {
         return ValueMatcherMode.MATCH_EXPECTED;
      }

      @Override
      public Set<Class<? extends MarshallableFunctions.SetValueIfEqualsReturnBoolean>> getTypeClasses() {
         return Util.<Class<? extends MarshallableFunctions.SetValueIfEqualsReturnBoolean>>asSet(MarshallableFunctions.SetValueIfEqualsReturnBoolean.class);
      }

      @Override
      public Integer getId() {
         return org.infinispan.commons.marshall.Ids.LAMBDA_SET_VALUE_IF_EQUALS_RETURN_BOOLEAN;
      }
   }

}
