package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;

import java.util.Collections;
import java.util.List;

/**
 * @param <TypeMetadata> is either java.lang.Class or com.google.protobuf.Descriptors.Descriptor
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterParsingResult<TypeMetadata> {

   private final BooleanExpr filter;
   private final String targetEntityName;
   private final TypeMetadata targetEntityMetadata;
   private final List<String> projections;

   public FilterParsingResult(BooleanExpr filter, String targetEntityName, TypeMetadata targetEntityMetadata,
                              List<String> projections) {
      this.filter = filter;
      this.targetEntityName = targetEntityName;
      this.targetEntityMetadata = targetEntityMetadata;
      this.projections = projections != null ? projections : Collections.<String>emptyList();
   }

   /**
    * Returns the filter created while walking the parse tree.
    *
    * @return the filter created while walking the parse tree
    */
   public BooleanExpr getQuery() {
      return filter;
   }

   /**
    * Returns the original entity name as given in the query
    *
    * @return the entity name of the query
    */
   public String getTargetEntityName() {
      return targetEntityName;
   }

   /**
    * Returns the entity type of the parsed query as derived from the queried entity name via the configured {@link
    * EntityNamesResolver}.
    *
    * @return the entity type of the parsed query
    */
   public TypeMetadata getTargetEntityMetadata() {
      return targetEntityMetadata;
   }

   /**
    * Returns the projections of the parsed query, represented as dot paths in case of references to fields of embedded
    * entities, e.g. {@code ["foo", "bar.qaz"]}.
    *
    * @return a list with the projections of the parsed query; an empty list will be returned if no the query has no
    * projections
    */
   public List<String> getProjections() {
      return projections;
   }

   @Override
   public String toString() {
      return "FilterParsingResult [filter=" + filter + ", targetEntityName=" + targetEntityName + ", projections=" + projections + "]";
   }
}
