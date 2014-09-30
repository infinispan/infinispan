package org.infinispan.cdi.util.annotatedtypebuilder;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Map;

import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.AnnotatedType;

/**
 * @author Stuart Douglas
 */
class AnnotatedMethodImpl<X> extends AnnotatedCallableImpl<X, Method> implements AnnotatedMethod<X> {

   AnnotatedMethodImpl(AnnotatedType<X> type, Method method, AnnotationStore annotations, Map<Integer, AnnotationStore> parameterAnnotations, Map<Integer, Type> parameterTypeOverrides) {
      super(type, method, method.getReturnType(), method.getParameterTypes(), method.getGenericParameterTypes(), annotations, parameterAnnotations, method.getGenericReturnType(), parameterTypeOverrides);
   }

}
