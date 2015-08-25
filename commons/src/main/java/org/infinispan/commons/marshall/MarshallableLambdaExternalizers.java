package org.infinispan.commons.marshall;

import org.infinispan.commons.util.Util;
import org.jboss.marshalling.util.IdentityIntMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import static org.infinispan.commons.marshall.MarshallableLambdas.*;

public class MarshallableLambdaExternalizers {

   public static final class ConstantLambdaExternalizer implements LambdaExternalizer<Object> {
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

      private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<>(12);

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
      }

      @Override
      public ValueMatcherMode valueMatcher(Object o) {
         int i = numbers.get(o.getClass(), -1);
         if (i > 0) {
            int valueMatcherId = ((i & VALUE_MATCH_MASK) >> 12) - 1;
            return ValueMatcherMode.values()[valueMatcherId];
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
            returnReadWriteView().getClass()
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
            default:
               throw new IllegalStateException("Unknown lambda ID: " + id);
         }
      }
   }

   public static final class SetValueIfEqualsReturnBooleanExternalizer
            implements LambdaExternalizer<SetValueIfEqualsReturnBoolean> {
      public void writeObject(ObjectOutput oo, SetValueIfEqualsReturnBoolean o) throws IOException {
         oo.writeObject(o.oldValue);
      }

      public SetValueIfEqualsReturnBoolean readObject(ObjectInput input)
         throws IOException, ClassNotFoundException {
         Object oldValue = input.readObject();
         return new SetValueIfEqualsReturnBoolean<>(oldValue);
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
