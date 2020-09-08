package org.hibernate.search.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.hibernate.search.engine.backend.types.ObjectStructure;
import org.hibernate.search.mapper.pojo.extractor.mapping.programmatic.ContainerExtractorPath;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMapping;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessor;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessorContext;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.processing.PropertyMappingAnnotationProcessorRef;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.PropertyMappingStep;

/**
 * Specifies that an association ({@code @*To*}, {@code @Embedded}, {@code @CollectionOfEmbedded}) is to be indexed in
 * the root entity index. This allows queries involving associated objects properties.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
@Documented
@PropertyMapping(processor = @PropertyMappingAnnotationProcessorRef(type = IndexedEmbedded.Processor.class))
public @interface IndexedEmbedded {

   /**
    * Field name prefix.
    *
    * @return the field name prefix. Default to ".", indicating that {@code propertyname.} will be used.
    */
   String prefix() default ".";

   /**
    * <p>List which <em>paths</em> of the object graph should be included
    * in the index, and need to match the field names used to store them in the index, so they will
    * also match the field names used to specify full text queries.</p>
    *
    * <p>Defined paths are going to be indexed even if they exceed the {@code depth} threshold.
    * When {@code includePaths} is not empty, the default value for {@code depth} is 0.</p>
    *
    * <p>Defined paths are implicitly prefixed with the {@link IndexedEmbedded#prefix()}.
    *
    * @return the paths to include. Default to empty array
    */
   String[] includePaths() default {};

   /**
    * Stop indexing embedded elements when {@code depth} is reached.
    * {@code depth=1} means the associated element is indexed, but not its embedded elements.
    *
    * <p>The default value depends on the value of the {@code includePaths} attribute: if no paths
    * are defined, the default is {@code Integer.MAX_VALUE}; if any {@code includePaths} are
    * defined, the default {@code depth} is interpreted as 0 if not specified to a different value
    * than it's default.</p>
    *
    * <p>Note that when defining any path to the {@code includePaths} attribute the default is zero also
    * when explicitly set to {@code Integer.MAX_VALUE}.</p>
    *
    * @return the depth size. Default value is {@link Integer#MAX_VALUE}
    */
   int depth() default Integer.MAX_VALUE;

   class Processor implements PropertyMappingAnnotationProcessor<IndexedEmbedded> {

      @Override
      public void process(PropertyMappingStep mappingContext, IndexedEmbedded annotation,
                          PropertyMappingAnnotationProcessorContext context) {
         // the following is an approximation. we're trying here to emulate the HS5's annotation behavior.
         // but some concepts of HS5 have no meaning with Search 6:

         String cleanedUpPrefix = annotation.prefix();
         if (cleanedUpPrefix.startsWith(".")) {
            cleanedUpPrefix = cleanedUpPrefix.substring(1);
         }

         if (cleanedUpPrefix.isEmpty()) {
            cleanedUpPrefix = null;
         }

         Integer cleanedUpMaxDepth = annotation.depth();
         if (cleanedUpMaxDepth.equals(-1)) {
            cleanedUpMaxDepth = null;
         }

         String[] includePathsArray = annotation.includePaths();
         Set<String> cleanedUpIncludePaths;
         if (includePathsArray.length > 0) {
            cleanedUpIncludePaths = new HashSet<>();
            Collections.addAll(cleanedUpIncludePaths, includePathsArray);
         } else {
            cleanedUpIncludePaths = Collections.emptySet();
         }

         mappingContext.indexedEmbedded(cleanedUpPrefix)
               .extractors(ContainerExtractorPath.defaultExtractors())
               .structure(ObjectStructure.DEFAULT)
               .includeDepth(cleanedUpMaxDepth)
               .includePaths(cleanedUpIncludePaths);
      }
   }
}
