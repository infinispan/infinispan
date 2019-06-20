package org.infinispan.marshall.core;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.MarshallingException;

/**
 * @author Ryan Emerson
 * @since 10.0
 */
class LambdaMarshaller {

   static void write(ObjectOutput out, Object o) throws IOException {
      try {
         Method writeReplace = o.getClass().getDeclaredMethod("writeReplace");
         writeReplace.setAccessible(true);
         SerializedLambda sl = (SerializedLambda) writeReplace.invoke(o);
         writeSerializedLamda(out, sl);
      } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
         throw new MarshallingException(e);
      }
   }

   private static void writeSerializedLamda(ObjectOutput out, SerializedLambda object) throws IOException {
      out.writeUTF(object.getCapturingClass());
      out.writeUTF(object.getFunctionalInterfaceClass());
      out.writeUTF(object.getFunctionalInterfaceMethodName());
      out.writeUTF(object.getFunctionalInterfaceMethodSignature());
      out.writeInt(object.getImplMethodKind());
      out.writeUTF(object.getImplClass());
      out.writeUTF(object.getImplMethodName());
      out.writeUTF(object.getImplMethodSignature());
      out.writeUTF(object.getInstantiatedMethodType());
      int numberOfArgs = object.getCapturedArgCount();
      MarshallUtil.marshallSize(out, numberOfArgs);
      for (int i = 0; i < numberOfArgs; i++)
         out.writeObject(object.getCapturedArg(i));
   }

   public static Object read(ObjectInput in) throws ClassNotFoundException, IOException {
      SerializedLambda sl = createSerializedLambda(in);
      try {
         Class clazz = Class.forName(sl.getCapturingClass().replace("/", "."));
         Method method = clazz.getDeclaredMethod("$deserializeLambda$", SerializedLambda.class);
         method.setAccessible(true);
         Object retVal = method.invoke(null, sl);
         return retVal;
      } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
         throw new MarshallingException(e);
      }
   }

   private static SerializedLambda createSerializedLambda(ObjectInput in) throws ClassNotFoundException, IOException {
      String clazz = in.readUTF().replace("/", ".");
      Class<?> capturingClass = Class.forName(clazz);
      String functionalInterfaceClass = in.readUTF();
      String functionalInterfaceMethodName = in.readUTF();
      String functionalInterfaceMethodSignature = in.readUTF();
      int implMethodKind = in.readInt();
      String implClass = in.readUTF();
      String implMethodName = in.readUTF();
      String implMethodSignature = in.readUTF();
      String instantiatedMethodType = in.readUTF();
      int numberOfArgs = MarshallUtil.unmarshallSize(in);
      Object[] args = new Object[numberOfArgs];
      for (int i = 0; i < numberOfArgs; i++)
         args[i] = in.readObject();

      return new SerializedLambda(capturingClass, functionalInterfaceClass, functionalInterfaceMethodName,
            functionalInterfaceMethodSignature, implMethodKind, implClass, implMethodName, implMethodSignature,
            instantiatedMethodType, args);
   }
}
