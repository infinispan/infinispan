package org.infinispan.marshall.core;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.functional.MetaParam;
import org.jboss.marshalling.util.IdentityIntMap;

public class MarshallableFunctionExternalizers {

   private static final int SET_VALUE_RETURN_PREV_OR_NULL = 1;
   private static final int SET_VALUE_RETURN_VIEW = 2;
   private static final int SET_VALUE_IF_ABSENT_RETURN_PREV_OR_NULL = 3;
   private static final int SET_VALUE_IF_ABSENT_RETURN_BOOLEAN = 4;
   private static final int SET_VALUE_IF_PRESENT_RETURN_PREV_OR_NULL = 5;
   private static final int SET_VALUE_IF_PRESENT_RETURN_BOOLEAN = 6;
   private static final int REMOVE_RETURN_PREV_OR_NULL = 7;
   private static final int REMOVE_RETURN_BOOLEAN = 8;
   private static final int REMOVE_IF_VALUE_EQUALS_RETURN_BOOLEAN = 9;
   private static final int SET_VALUE_CONSUMER = 10;
   private static final int REMOVE_CONSUMER = 11;
   private static final int RETURN_READ_WRITE_FIND = 12;
   private static final int RETURN_READ_WRITE_GET = 13;
   private static final int RETURN_READ_WRITE_VIEW = 14;
   private static final int RETURN_READ_ONLY_FIND_OR_NULL = 15;
   private static final int RETURN_READ_ONLY_FIND_IS_PRESENT = 16;
   private static final int IDENTITY = 17;
   private static final int SET_INTERNAL_CACHE_VALUE_CONSUMER = 18;
   private static final int MAP_KEY = 19;

   public static final class ConstantLambdaExternalizer implements AdvancedExternalizer<Object> {
      private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<>(16);
      private final Set<Class<?>> classes = new HashSet<>(32);

      public ConstantLambdaExternalizer() {
         add(MarshallableFunctions.setValueReturnPrevOrNull().getClass(), SET_VALUE_RETURN_PREV_OR_NULL);
         add(MarshallableFunctions.setValueReturnView().getClass(), SET_VALUE_RETURN_VIEW);
         add(MarshallableFunctions.setValueIfAbsentReturnPrevOrNull().getClass(), SET_VALUE_IF_ABSENT_RETURN_PREV_OR_NULL);
         add(MarshallableFunctions.setValueIfAbsentReturnBoolean().getClass(), SET_VALUE_IF_ABSENT_RETURN_BOOLEAN);
         add(MarshallableFunctions.setValueIfPresentReturnPrevOrNull().getClass(), SET_VALUE_IF_PRESENT_RETURN_PREV_OR_NULL);
         add(MarshallableFunctions.setValueIfPresentReturnBoolean().getClass(), SET_VALUE_IF_PRESENT_RETURN_BOOLEAN);
         add(MarshallableFunctions.removeReturnPrevOrNull().getClass(), REMOVE_RETURN_PREV_OR_NULL);
         add(MarshallableFunctions.removeReturnBoolean().getClass(), REMOVE_RETURN_BOOLEAN);
         add(MarshallableFunctions.removeIfValueEqualsReturnBoolean().getClass(), REMOVE_IF_VALUE_EQUALS_RETURN_BOOLEAN);
         add(MarshallableFunctions.setValueConsumer().getClass(), SET_VALUE_CONSUMER);
         add(MarshallableFunctions.removeConsumer().getClass(), REMOVE_CONSUMER);
         add(MarshallableFunctions.returnReadWriteFind().getClass(), RETURN_READ_WRITE_FIND);
         add(MarshallableFunctions.returnReadWriteGet().getClass(), RETURN_READ_WRITE_GET);
         add(MarshallableFunctions.returnReadWriteView().getClass(), RETURN_READ_WRITE_VIEW);
         add(MarshallableFunctions.returnReadOnlyFindOrNull().getClass(), RETURN_READ_ONLY_FIND_OR_NULL);
         add(MarshallableFunctions.returnReadOnlyFindIsPresent().getClass(), RETURN_READ_ONLY_FIND_IS_PRESENT);
         add(MarshallableFunctions.identity().getClass(), IDENTITY);
         add(MarshallableFunctions.setInternalCacheValueConsumer().getClass(), SET_INTERNAL_CACHE_VALUE_CONSUMER);
         add(MarshallableFunctions.mapKey().getClass(), MAP_KEY);
      }

      protected void add(Class<?> clazz, int id) {
         numbers.put(clazz, id);
         classes.add(clazz);
      }

      @Override
      public Set<Class<?>> getTypeClasses() {
         return classes;
      }

      @Override
      public Integer getId() {
         return org.infinispan.commons.marshall.Ids.LAMBDA_CONSTANT;
      }

      public void writeObject(ObjectOutput oo, Object o) throws IOException {
         int id = numbers.get(o.getClass(), -1);
         oo.writeByte(id);
      }

      public Object readObject(ObjectInput input) throws IOException {
         short id = input.readByte();
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
            case MAP_KEY: return MarshallableFunctions.mapKey();
            default:
               throw new IllegalStateException("Unknown lambda ID: " + id);
         }
      }
   }

   public static final class LambdaWithMetasExternalizer implements AdvancedExternalizer<MarshallableFunctions.LambdaWithMetas> {
      private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<>(8);
      private final Set<Class<? extends MarshallableFunctions.LambdaWithMetas>> classes = new HashSet<>(16);

      public LambdaWithMetasExternalizer() {
         add(MarshallableFunctions.SetValueMetasReturnPrevOrNull.class, SET_VALUE_RETURN_PREV_OR_NULL);
         add(MarshallableFunctions.SetValueMetasReturnView.class, SET_VALUE_RETURN_VIEW);
         add(MarshallableFunctions.SetValueMetasIfAbsentReturnPrevOrNull.class, SET_VALUE_IF_ABSENT_RETURN_PREV_OR_NULL);
         add(MarshallableFunctions.SetValueMetasIfAbsentReturnBoolean.class, SET_VALUE_IF_ABSENT_RETURN_BOOLEAN);
         add(MarshallableFunctions.SetValueMetasIfPresentReturnPrevOrNull.class, SET_VALUE_IF_PRESENT_RETURN_PREV_OR_NULL);
         add(MarshallableFunctions.SetValueMetasIfPresentReturnBoolean.class, SET_VALUE_IF_PRESENT_RETURN_BOOLEAN);
         add(MarshallableFunctions.SetValueMetas.class, SET_VALUE_CONSUMER);
      }

      protected void add(Class<? extends MarshallableFunctions.LambdaWithMetas> klazz, int id) {
         numbers.put(klazz, id);
         classes.add(klazz);
      }

      @Override
      public Set<Class<? extends MarshallableFunctions.LambdaWithMetas>> getTypeClasses() {
         return classes;
      }

      @Override
      public Integer getId() {
         return org.infinispan.commons.marshall.Ids.LAMBDA_WITH_METAS;
      }

      @Override
      public void writeObject(ObjectOutput oo, MarshallableFunctions.LambdaWithMetas o) throws IOException {
         int id = numbers.get(o.getClass(), -1);
         oo.writeByte(id);
         writeMetas(oo, o);
      }

      @Override
      public MarshallableFunctions.LambdaWithMetas readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         short id = input.readByte();
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
            implements AdvancedExternalizer<MarshallableFunctions.SetValueIfEqualsReturnBoolean> {
      public void writeObject(ObjectOutput oo, MarshallableFunctions.SetValueIfEqualsReturnBoolean o) throws IOException {
         oo.writeObject(o.oldValue);
         writeMetas(oo, o);
      }

      public MarshallableFunctions.SetValueIfEqualsReturnBoolean readObject(ObjectInput input)
         throws IOException, ClassNotFoundException {
         Object oldValue = input.readObject();
         MetaParam.Writable[] metas = readMetas(input);
         return new MarshallableFunctions.SetValueIfEqualsReturnBoolean<>(oldValue, metas);
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
