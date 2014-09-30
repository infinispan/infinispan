package org.infinispan.cdi.util.annotatedtypebuilder;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.infinispan.cdi.util.Reflections;
import org.infinispan.cdi.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Helper class used to build annotation stores
 *
 * @author Stuart Douglas
 */
public class AnnotationBuilder {

   private static final Log log = LogFactory.getLog(AnnotationBuilder.class, Log.class);

   private final Map<Class<? extends Annotation>, Annotation> annotationMap;
   private final Set<Annotation> annotationSet;

   AnnotationBuilder() {
      this.annotationMap = new HashMap<Class<? extends Annotation>, Annotation>();
      this.annotationSet = new HashSet<Annotation>();
   }

   public AnnotationBuilder add(Annotation annotation) {
      if (annotation == null) {
         throw log.parameterMustNotBeNull("annotation");
      }
      annotationSet.add(annotation);
      annotationMap.put(annotation.annotationType(), annotation);
      return this;
   }

   public AnnotationBuilder remove(Class<? extends Annotation> annotationType) {
      if (annotationType == null) {
         throw log.parameterMustNotBeNull("annotationType");
      }

      Iterator<Annotation> it = annotationSet.iterator();
      while (it.hasNext()) {
         Annotation an = it.next();
         if (annotationType.isAssignableFrom(an.annotationType())) {
            it.remove();
         }
      }
      annotationMap.remove(annotationType);
      return this;
   }

   AnnotationStore create() {
      return new AnnotationStore(annotationMap, annotationSet);
   }

   public AnnotationBuilder addAll(AnnotationStore annotations) {
      for (Annotation annotation : annotations.getAnnotations()) {
         add(annotation);
      }
      return this;
   }

   public <T extends Annotation> T getAnnotation(Class<T> anType) {
      return Reflections.cast(annotationMap.get(anType));
   }

   public boolean isAnnotationPresent(Class<?> annotationType) {
      return annotationMap.containsKey(annotationType);
   }

   @Override
   public String toString() {
      return annotationSet.toString();
   }

}
