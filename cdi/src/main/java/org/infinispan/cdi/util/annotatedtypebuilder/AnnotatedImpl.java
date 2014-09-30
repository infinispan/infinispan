package org.infinispan.cdi.util.annotatedtypebuilder;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.enterprise.inject.spi.Annotated;

import org.infinispan.cdi.util.HierarchyDiscovery;

/**
 * The base class for all New Annotated types.
 *
 * @author Stuart Douglas
 */
abstract class AnnotatedImpl implements Annotated {

   private final Type type;
   private final Set<Type> typeClosure;
   private final AnnotationStore annotations;

   protected AnnotatedImpl(Class<?> type, AnnotationStore annotations, Type genericType, Type overridenType) {
      if (overridenType == null) {
         if (genericType != null) {
            typeClosure = new HierarchyDiscovery(genericType).getTypeClosure();
            this.type = genericType;
         } else {
            typeClosure = new HierarchyDiscovery(type).getTypeClosure();
            this.type = type;
         }
      } else {
         this.type = overridenType;
         this.typeClosure = Collections.singleton(overridenType);
      }


      if (annotations == null) {
         this.annotations = new AnnotationStore();
      } else {
         this.annotations = annotations;
      }
   }

   public <T extends Annotation> T getAnnotation(Class<T> annotationType) {
      return annotations.getAnnotation(annotationType);
   }

   public Set<Annotation> getAnnotations() {
      return annotations.getAnnotations();
   }

   public boolean isAnnotationPresent(Class<? extends Annotation> annotationType) {
      return annotations.isAnnotationPresent(annotationType);
   }

   public Set<Type> getTypeClosure() {
      return new HashSet<Type>(typeClosure);
   }

   public Type getBaseType() {
      return type;
   }

}
