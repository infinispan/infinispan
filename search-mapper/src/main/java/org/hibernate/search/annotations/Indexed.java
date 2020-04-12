package org.hibernate.search.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.TypeMapping;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.TypeMappingAnnotationProcessor;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.TypeMappingAnnotationProcessorContext;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.TypeMappingAnnotationProcessorRef;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.TypeMappingStep;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
@TypeMapping(processor = @TypeMappingAnnotationProcessorRef(type = Indexed.Processor.class))
/**
 * Specifies that an entity is to be indexed by Lucene
 */
public @interface Indexed {

   /**
    * @return The filename of the index. Default to empty string
    */
   String index() default "";

   class Processor implements TypeMappingAnnotationProcessor<Indexed> {
      // the following is an approximation. we're trying here to emulate the HS5's annotation behavior.
      // but some concepts of HS5 have no meaning with Search 6:

      @Override
      public void process(TypeMappingStep mapping, Indexed annotation, TypeMappingAnnotationProcessorContext context) {
         String indexName = annotation.index();
         if ( indexName.isEmpty() ) {
            indexName = null;
         }
         mapping.indexed().index( indexName );
      }
   }
}
