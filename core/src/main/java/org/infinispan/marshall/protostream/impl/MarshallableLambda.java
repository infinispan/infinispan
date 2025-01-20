package org.infinispan.marshall.protostream.impl;

import java.lang.invoke.SerializedLambda;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A wrapper object that allows a generic {@link java.io.Serializable} lambda to be marshalled by wrapping the contents
 * of a {@link SerializedLambda} object.
 *
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MARSHALLABLE_LAMBDA)
public class MarshallableLambda {

   @ProtoField(number = 1)
   final String capturingClass;

   @ProtoField(number = 2)
   final String functionalInterfaceClass;

   @ProtoField(number = 3)
   final String functionalInterfaceMethodName;

   @ProtoField(number = 4)
   final String functionalInterfaceMethodSignature;

   @ProtoField(number = 5, defaultValue = "-1")
   int implMethodKind;

   @ProtoField(number = 6)
   final String implClass;

   @ProtoField(number = 7)
   final String implMethodName;

   @ProtoField(number = 8)
   final String implMethodSignature;

   @ProtoField(number = 9)
   final String instantiatedMethodType;

   @ProtoField(number = 10)
   final MarshallableArray<Object> arguments;

   @ProtoFactory
   MarshallableLambda(String capturingClass, String functionalInterfaceClass,
                      String functionalInterfaceMethodName, String functionalInterfaceMethodSignature,
                      int implMethodKind, String implClass, String implMethodName, String implMethodSignature,
                      String instantiatedMethodType, MarshallableArray<Object> arguments) {
      this.capturingClass = capturingClass;
      this.functionalInterfaceClass = functionalInterfaceClass;
      this.functionalInterfaceMethodName = functionalInterfaceMethodName;
      this.functionalInterfaceMethodSignature = functionalInterfaceMethodSignature;
      this.implMethodKind = implMethodKind;
      this.implClass = implClass;
      this.implMethodName = implMethodName;
      this.implMethodSignature = implMethodSignature;
      this.instantiatedMethodType = instantiatedMethodType;
      this.arguments = arguments;
   }

   public static MarshallableLambda create(Object o) {
      try {
         Method writeReplace = o.getClass().getDeclaredMethod("writeReplace");
         writeReplace.setAccessible(true);
         SerializedLambda sl = (SerializedLambda) writeReplace.invoke(o);

         int numberOfArgs = sl.getCapturedArgCount();
         Object[] args = new Object[numberOfArgs];
         for (int i = 0; i < numberOfArgs; i++)
            args[i] = sl.getCapturedArg(i);
         MarshallableArray<Object> wrappedArgs = MarshallableArray.create(args);

         return new MarshallableLambda(
               sl.getCapturingClass().replace("/", "."),
               sl.getFunctionalInterfaceClass(),
               sl.getFunctionalInterfaceMethodName(),
               sl.getFunctionalInterfaceMethodSignature(),
               sl.getImplMethodKind(),
               sl.getImplClass(),
               sl.getImplMethodName(),
               sl.getImplMethodSignature(),
               sl.getInstantiatedMethodType(),
               wrappedArgs
         );
      } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
         throw new MarshallingException(e);
      }
   }

   public Object unwrap(ClassLoader classLoader) {
      try {
         Class<?> clazz = Class.forName(capturingClass, true, classLoader);
         Object[] args = MarshallableArray.unwrap(arguments, new Object[0]);

         SerializedLambda sl = new SerializedLambda(clazz, functionalInterfaceClass, functionalInterfaceMethodName,
               functionalInterfaceMethodSignature, implMethodKind, implClass, implMethodName, implMethodSignature,
               instantiatedMethodType, args);

         Method method = clazz.getDeclaredMethod("$deserializeLambda$", SerializedLambda.class);
         method.setAccessible(true);
         return method.invoke(null, sl);
      } catch (ClassNotFoundException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
         throw new MarshallingException(e);
      }
   }
}
