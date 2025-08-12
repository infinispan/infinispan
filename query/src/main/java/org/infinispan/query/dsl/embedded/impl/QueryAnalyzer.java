package org.infinispan.query.dsl.embedded.impl;

import java.util.Arrays;

import org.infinispan.query.objectfilter.SortField;
import org.infinispan.query.objectfilter.impl.ql.PropertyPath;
import org.infinispan.query.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.query.objectfilter.impl.syntax.IndexedFieldProvider;
import org.infinispan.query.objectfilter.impl.syntax.parser.AggregationPropertyPath;
import org.infinispan.query.objectfilter.impl.syntax.parser.CacheValueAggregationPropertyPath;
import org.infinispan.query.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.objectfilter.impl.syntax.parser.ObjectPropertyHelper;

public final class QueryAnalyzer<TypeMetadata> {

   private final ObjectPropertyHelper<TypeMetadata> propertyHelper;

   public QueryAnalyzer(ObjectPropertyHelper<TypeMetadata> propertyHelper) {
      this.propertyHelper = propertyHelper;
   }

   public boolean fullIndexingAggregation(IckleParsingResult<TypeMetadata> parsingResult) {
      BooleanExpr havingClause = parsingResult.getHavingClause();
      if (havingClause != null) {
         return false;
      }

      PropertyPath[] groupBys = parsingResult.getGroupBy();
      if (groupBys == null || groupBys.length != 1) {
         return false;
      }

      IndexedFieldProvider.FieldIndexingMetadata metadata =
            propertyHelper.getIndexedFieldProvider().get(parsingResult.getTargetEntityMetadata());

      PropertyPath groupBy = groupBys[0];
      String[] groupByPath = groupBy.asArrayPath();
      if (!metadata.isAggregable(groupByPath)) {
         return false;
      }

      PropertyPath[] projectedPaths = parsingResult.getProjectedPaths();
      AggregationPropertyPath aggregation = null;
      PropertyPath projection = null;

      // we need at least one aggregation and one projection (the latter targeting the group by path)
      for (PropertyPath projectedPath : projectedPaths) {
         if (projectedPath instanceof AggregationPropertyPath) {
            if (aggregation != null) {
               // we support only one aggregation on indexed query
               return false;
            }
            aggregation = (AggregationPropertyPath) projectedPath;

            boolean aggregationTargetsAProperty = propertyHelper.hasProperty(parsingResult.getTargetEntityMetadata(), aggregation.asArrayPath());
            if (aggregationTargetsAProperty && !(aggregation instanceof CacheValueAggregationPropertyPath)) {
               if (!metadata.isSearchable(aggregation.asArrayPath())) {
                  return false;
               }
            }
         } else {
            projection = projectedPath;
            if (!Arrays.equals(groupByPath, projection.asArrayPath())) {
               return false;
            }
         }
      }
      if (aggregation == null || projection == null) {
         return false;
      }

      SortField[] sortFields = parsingResult.getSortFields();
      if (sortFields == null || sortFields.length == 0) {
         return true;
      }

      // check that sorting is on the group by path and the path is sortable
      if (sortFields.length > 1) {
         return false;
      }
      if (!Arrays.equals(groupByPath, sortFields[0].getPath().asArrayPath())) {
         return false;
      }
      return metadata.isSortable(sortFields[0].getPath().asArrayPath());
   }
}
