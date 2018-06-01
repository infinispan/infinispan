/*
 * Copyright 2016, Red Hat Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.infinispan.objectfilter.impl.ql;

import java.util.List;

import org.antlr.runtime.tree.Tree;

/**
 * Defines hooks for implementing custom logic when walking the parse tree of a JPQL query.
 *
 * @author Gunnar Morling
 * @author Adrian Nistor
 * @since 9.0
 */
public interface QueryRendererDelegate<TypeDescriptor> {

   void registerPersisterSpace(String entityName, Tree aliasTree);

   void registerJoinAlias(Tree aliasTree, PropertyPath<TypeDescriptor> path);

   boolean isUnqualifiedPropertyReference();

   boolean isPersisterReferenceAlias();

   void activateFromStrategy(JoinType joinType, Tree associationFetchTree, Tree propertyFetchTree, Tree aliasTree);

   void activateSelectStrategy();

   void activateWhereStrategy();

   void activateGroupByStrategy();

   void activateHavingStrategy();

   void activateOrderByStrategy();

   void deactivateStrategy();

   void activateOR();

   void activateAND();

   void activateNOT();

   void deactivateBoolean();

   void predicateLess(String value);

   void predicateLessOrEqual(String value);

   void predicateEquals(String value);

   void predicateNotEquals(String value);

   void predicateGreaterOrEqual(String value);

   void predicateGreater(String value);

   void predicateBetween(String lowerValue, String upperValue);

   void predicateIn(List<String> values);

   void predicateLike(String patternValue, Character escapeCharacter);

   void predicateIsNull();

   void predicateConstantBoolean(boolean booleanConstant);

   void predicateGeodist(String latitude, String longitude);

   void predicateGeofilt(String latitude, String longitude, String radius);

   void predicateFullTextTerm(String term, String fuzzyFlop);

   void predicateFullTextRegexp(String term);

   void predicateFullTextRange(boolean includeLower, String lower, String upper, boolean includeUpper);

   enum Occur {
      MUST("+"),
      FILTER("#"),
      SHOULD(""),
      MUST_NOT("-");

      private final String operator;

      Occur(String operator) {
         this.operator = operator;
      }

      public String getOperator() {
         return operator;
      }
   }

   void activateFullTextOccur(Occur occur);

   void deactivateFullTextOccur();

   void activateFullTextBoost(float boost);

   void deactivateFullTextBoost();

   void activateAggregation(AggregationFunction aggregationFunction);

   void deactivateAggregation();

   void activateSpatial(SpatialFunction spatialFunction);

   void deactivateSpatial();

   /**
    * @param collateName optional collation name
    */
   void groupingValue(String collateName);

   /**
    * Sets the sort direction, either "asc" or "desc", for the current property. The property was previously
    * specified by {@link #setPropertyPath(PropertyPath)}
    *
    * @param collateName optional collation name
    * @param isAscending indicates if sorting is ascending or descending
    */
   void sortSpecification(String collateName, boolean isAscending);

   /**
    * Sets a property path representing one property in the SELECT, GROUP BY, WHERE or HAVING clause of a given query.
    *
    * @param propertyPath the property path to set
    */
   void setPropertyPath(PropertyPath<TypeDescriptor> propertyPath);
}
