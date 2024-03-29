package org.infinispan.cdi.common.util.annotatedtypebuilder;

import java.lang.reflect.Type;

import jakarta.enterprise.inject.spi.AnnotatedCallable;
import jakarta.enterprise.inject.spi.AnnotatedParameter;

/**
 * @author Stuart Douglas
 */
class AnnotatedParameterImpl<X> extends AnnotatedImpl implements AnnotatedParameter<X> {

   private final int position;
   private final AnnotatedCallable<X> declaringCallable;

   AnnotatedParameterImpl(AnnotatedCallable<X> declaringCallable, Class<?> type, int position, AnnotationStore annotations, Type genericType, Type typeOverride) {
      super(type, annotations, genericType, typeOverride);
      this.declaringCallable = declaringCallable;
      this.position = position;
   }

   public AnnotatedCallable<X> getDeclaringCallable() {
      return declaringCallable;
   }

   public int getPosition() {
      return position;
   }

}
