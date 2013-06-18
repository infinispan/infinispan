package org.infinispan.cdi.util.annotatedtypebuilder;

import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;

/**
 * A helper class used to hold annotations on a type or member.
 *
 * @author Stuart Douglas
 */
class AnnotationStore {

   private final Map<Class<? extends Annotation>, Annotation> annotationMap;
   private final Set<Annotation> annotationSet;

   AnnotationStore(Map<Class<? extends Annotation>, Annotation> annotationMap, Set<Annotation> annotationSet) {
      this.annotationMap = annotationMap;
      this.annotationSet = unmodifiableSet(annotationSet);
   }

   AnnotationStore() {
      this.annotationMap = emptyMap();
      this.annotationSet = emptySet();
   }

   <T extends Annotation> T getAnnotation(Class<T> annotationType) {
      return annotationType.cast(annotationMap.get(annotationType));
   }

   Set<Annotation> getAnnotations() {
      return annotationSet;
   }

   boolean isAnnotationPresent(Class<? extends Annotation> annotationType) {
      return annotationMap.containsKey(annotationType);
   }

}
