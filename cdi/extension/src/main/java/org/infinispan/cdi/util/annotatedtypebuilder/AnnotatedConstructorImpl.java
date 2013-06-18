package org.infinispan.cdi.util.annotatedtypebuilder;

import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.util.Map;

import javax.enterprise.inject.spi.AnnotatedConstructor;

/**
 * @author Stuart Douglas
 */
class AnnotatedConstructorImpl<X> extends AnnotatedCallableImpl<X, Constructor<X>> implements AnnotatedConstructor<X> {

   AnnotatedConstructorImpl(AnnotatedTypeImpl<X> type, Constructor<?> constructor, AnnotationStore annotations, Map<Integer, AnnotationStore> parameterAnnotations, Map<Integer, Type> typeOverrides) {
      super(type, (Constructor<X>) constructor, constructor.getDeclaringClass(), constructor.getParameterTypes(), getGenericArray(constructor), annotations, parameterAnnotations, null, typeOverrides);
   }

   private static Type[] getGenericArray(Constructor<?> constructor) {
      Type[] genericTypes = constructor.getGenericParameterTypes();
      // for inner classes genericTypes and parameterTypes can be different
      // length, this is a hack to fix this.
      // TODO: investigate this behavior further, on different JVM's and
      // compilers
      if (genericTypes.length + 1 == constructor.getParameterTypes().length) {
         genericTypes = new Type[constructor.getGenericParameterTypes().length + 1];
         genericTypes[0] = constructor.getParameterTypes()[0];
         for (int i = 0; i < constructor.getGenericParameterTypes().length; ++i) {
            genericTypes[i + 1] = constructor.getGenericParameterTypes()[i];
         }
      }
      return genericTypes;
   }

}
