package org.infinispan.query.dsl.embedded.impl.jpalucene;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;

import java.util.Arrays;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class LuceneQueryParsingResult<TypeMetadata> {

   private final Query query;
   private final String targetEntityName;
   private final TypeMetadata targetEntityMetadata;
   private final String[] projections;
   private final Sort sort;

   LuceneQueryParsingResult(Query query, String targetEntityName, TypeMetadata targetEntityMetadata, String[] projections, Sort sort) {
      this.query = query;
      this.targetEntityName = targetEntityName;
      this.targetEntityMetadata = targetEntityMetadata;
      this.projections = projections;
      this.sort = sort;
   }

   /**
    * Returns the created Lucene query.
    */
   public Query getQuery() {
      return query;
   }

   /**
    * Returns the original entity name from the query.
    */
   public String getTargetEntityName() {
      return targetEntityName;
   }

   /**
    * Returns the entity metadata (usually a Class) resolved from the original entity name.
    */
   public TypeMetadata getTargetEntityMetadata() {
      return targetEntityMetadata;
   }

   /**
    * Returns the projections of the parsed query, represented as dot-separated paths to fields of embedded entities.
    *
    * @return an array with the projections of the parsed query or {@code null} if the query has no projections
    */
   public String[] getProjections() {
      return projections;
   }

   /**
    * Returns the optional Lucene sort specification.
    *
    * @return the {@link Sort} object or {@code null} if the query string does not specify sorting
    */
   public Sort getSort() {
      return sort;
   }

   @Override
   public String toString() {
      return "LuceneQueryParsingResult [query=" + query + ", targetEntityMetadata=" + targetEntityMetadata
            + ", projections=" + Arrays.toString(projections) + ", sort=" + sort + "]";
   }
}
