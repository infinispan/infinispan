package org.infinispan.cdi.util.annotatedtypebuilder;

import java.lang.reflect.Member;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.enterprise.inject.spi.AnnotatedCallable;
import javax.enterprise.inject.spi.AnnotatedParameter;
import javax.enterprise.inject.spi.AnnotatedType;

/**
 * @author Stuart Douglas
 */
abstract class AnnotatedCallableImpl<X, Y extends Member> extends AnnotatedMemberImpl<X, Y> implements AnnotatedCallable<X> {

   private final List<AnnotatedParameter<X>> parameters;

   protected AnnotatedCallableImpl(AnnotatedType<X> declaringType, Y member, Class<?> memberType, Class<?>[] parameterTypes, Type[] genericTypes, AnnotationStore annotations, Map<Integer, AnnotationStore> parameterAnnotations, Type genericType, Map<Integer, Type> parameterTypeOverrides) {
      super(declaringType, member, memberType, annotations, genericType, null);
      this.parameters = getAnnotatedParameters(this, parameterTypes, genericTypes, parameterAnnotations, parameterTypeOverrides);
   }

   public List<AnnotatedParameter<X>> getParameters() {
      return Collections.unmodifiableList(parameters);
   }

   private static <X, Y extends Member> List<AnnotatedParameter<X>> getAnnotatedParameters(AnnotatedCallableImpl<X, Y> callable, Class<?>[] parameterTypes, Type[] genericTypes, Map<Integer, AnnotationStore> parameterAnnotations, Map<Integer, Type> parameterTypeOverrides) {
      List<AnnotatedParameter<X>> parameters = new ArrayList<AnnotatedParameter<X>>();
      int len = parameterTypes.length;
      for (int i = 0; i < len; ++i) {
         AnnotationBuilder builder = new AnnotationBuilder();
         if (parameterAnnotations != null && parameterAnnotations.containsKey(i)) {
            builder.addAll(parameterAnnotations.get(i));
         }
         Type over = null;
         if (parameterTypeOverrides != null) {
            over = parameterTypeOverrides.get(i);
         }
         AnnotatedParameterImpl<X> p = new AnnotatedParameterImpl<X>(callable, parameterTypes[i], i, builder.create(), genericTypes[i], over);
         parameters.add(p);
      }
      return parameters;
   }

}
