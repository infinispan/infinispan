package org.hibernate.search.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.invoke.MethodHandles;
import java.util.Optional;

import org.hibernate.search.engine.backend.types.Aggregable;
import org.hibernate.search.engine.backend.types.Projectable;
import org.hibernate.search.engine.backend.types.Searchable;
import org.hibernate.search.engine.backend.types.Sortable;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.MappingAnnotatedProperty;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMapping;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessor;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessorContext;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessorRef;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.PropertyMappingGenericFieldOptionsStep;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.PropertyMappingStandardFieldOptionsStep;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.PropertyMappingStep;
import org.hibernate.search.util.common.logging.impl.LoggerFactory;
import org.infinispan.search.mapper.log.impl.Log;
import org.infinispan.search.mapper.mapping.impl.DefaultAnalysisConfigurer;

/**
 * Annotation used for marking a property as indexable.
 *
 * @author Emmanuel Bernard
 * @author Hardy Ferentschik
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
@Documented
@Repeatable(Fields.class)
@PropertyMapping(processor = @PropertyMappingAnnotationProcessorRef(type = Field.Processor.class))
public @interface Field {

   /**
    * Default value for the {@link #indexNullAs} parameter. Indicates that {@code null} values should not be indexed.
    */
   String DO_NOT_INDEX_NULL = "__DO_NOT_INDEX_NULL__";

   /**
    * Value for the {@link #indexNullAs} parameter indicating that {@code null} values should be indexed using the null
    * token. If no value is given for that property, the token {@code _null_} will be used.
    */
   String DEFAULT_NULL_TOKEN = "__DEFAULT_NULL_TOKEN__";

   /**
    * @return Returns the field name. Defaults to the JavaBean property name.
    */
   String name() default "";

   /**
    * @return Returns a {@code Store} enum type indicating whether the value should be stored in the document. Defaults
    * to {@code Store.NO}.
    */
   Store store() default Store.NO;

   /**
    * @return Returns a {@code Index} enum defining whether the value should be indexed or not. Defaults to {@code
    * Index.YES}.
    */
   Index index() default Index.YES;

   /**
    * @return Returns a {@code Analyze} enum defining whether the value should be analyzed or not. Defaults to {@code
    * Analyze.YES}.
    */
   Analyze analyze() default Analyze.YES;

   /**
    * @return Returns the value to be used for indexing {@code null}. Per default {@code Field.NO_NULL_INDEXING} is
    * returned indicating that null values are not indexed.
    */
   String indexNullAs() default DO_NOT_INDEX_NULL;

   class Processor implements PropertyMappingAnnotationProcessor<Field> {

      private static final Log log = LoggerFactory.make(Log.class, MethodHandles.lookup());

      @Override
      public void process(PropertyMappingStep mapping, Field annotation,
                          PropertyMappingAnnotationProcessorContext context) {
         // the following is an approximation. we're trying here to emulate the HS5's annotation behavior.
         // but some concepts of HS5, such as `DEFAULT_NULL_TOKEN`, have no meaning for Search 6.

         MappingAnnotatedProperty annotatedProperty = context.annotatedElement();
         Class<?> propertyType = annotatedProperty.javaClass();
         String name = annotation.name().isEmpty() ? null : annotation.name();

         boolean sortable = annotatedProperty.allAnnotations()
               .filter(a -> SortableField.class.equals(a.annotationType()))
               .map(a -> ((SortableField) a).forField())
               .anyMatch(a -> a.isEmpty() || annotatedProperty.name().equals(a));

         boolean analyzed = Analyze.YES.equals(annotation.analyze()) && String.class.equals(propertyType);
         if (analyzed && sortable) {
            throw log.fieldSortableAndAnalyzed(annotatedProperty.name());
         }

         String indexNullAs = annotation.indexNullAs();
         if (DEFAULT_NULL_TOKEN.equals(indexNullAs)) {
            throw log.defaultNullTokenNotSupported(annotatedProperty.name());
         }

         PropertyMappingStandardFieldOptionsStep optionsStep;
         if (analyzed) {
            Optional<String> analyzerAnnotation = annotatedProperty.allAnnotations()
                  .filter(a -> Analyzer.class.equals(a.annotationType()))
                  .map(a -> ((Analyzer) a).definition())
                  .filter(a -> !a.isEmpty())
                  .findAny();

            String analyzer = (analyzerAnnotation.isPresent()) ? analyzerAnnotation.get() :
                  DefaultAnalysisConfigurer.STANDARD_ANALYZER_NAME;
            optionsStep = mapping.fullTextField(name).analyzer(analyzer);
         } else {
            PropertyMappingGenericFieldOptionsStep genericOptionsStep = mapping.genericField(name)
                  .sortable((sortable) ? Sortable.YES : Sortable.NO)
                  .aggregable(Aggregable.NO);

            if (!DO_NOT_INDEX_NULL.equals(indexNullAs)) {
               genericOptionsStep.indexNullAs(indexNullAs);
            }
            optionsStep = genericOptionsStep;
         }

         optionsStep
               .searchable(Index.YES.equals(annotation.index()) ? Searchable.YES : Searchable.NO)
               .projectable(Store.YES.equals(annotation.store()) ? Projectable.YES : Projectable.NO);
      }
   }
}
