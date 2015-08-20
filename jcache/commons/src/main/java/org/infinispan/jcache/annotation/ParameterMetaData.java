package org.infinispan.jcache.annotation;

import static java.util.Collections.unmodifiableSet;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Set;

/**
 * Contains the metadata for a parameter of a method annotated with A JCACHE annotation.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class ParameterMetaData {

   private final Type baseType;
   private final Class<?> rawType;
   private final int position;
   private final Set<Annotation> annotations;

   public ParameterMetaData(Class<?> type, int position, Set<Annotation> annotations) {
      this.baseType = type.getGenericSuperclass();
      this.rawType = type;
      this.position = position;
      this.annotations = unmodifiableSet(annotations);
   }

   public Class<?> getRawType() {
      return rawType;
   }

   public int getPosition() {
      return position;
   }

   public Set<Annotation> getAnnotations() {
      return annotations;
   }

   @Override
   public String toString() {
      return new StringBuilder()
            .append("ParameterMetaData{")
            .append("baseType=").append(baseType)
            .append(", rawType=").append(rawType)
            .append(", position=").append(position)
            .append(", annotations=").append(annotations)
            .append('}')
            .toString();
   }
}
