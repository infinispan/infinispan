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

tree grammar QueryRenderer;

options {
	output=AST;
	rewrite=true;
	tokenVocab=IckleLexer;
	ASTLabelType=CommonTree;
	TokenLabelType=CommonToken;
}

@header {
package org.infinispan.objectfilter.impl.ql.parse;

import org.infinispan.objectfilter.impl.ql.*;
}

@members {
  private QueryRendererDelegate delegate;

  public QueryRenderer(TreeNodeStream input, QueryRendererDelegate delegate) {
    this(input, new RecognizerSharedState());
    this.delegate = delegate;
  }
}

statement
	:
	queryStatementSet
	;

queryStatementSet
	:	queryStatement+
	;

queryStatement
	:	^(QUERY querySpec orderByClause?)
	;

querySpec
	:	^(QUERY_SPEC selectFrom whereClause? groupByClause? havingClause?)
	;

whereClause
	:	{ delegate.activateWhereStrategy(); } ^(WHERE searchCondition) { delegate.deactivateStrategy(); }
	;

groupByClause
	:	{ delegate.activateGroupByStrategy(); } ^(GROUP_BY groupingValue+) { delegate.deactivateStrategy(); }
	;

groupingValue
	:	^(GROUPING_VALUE valueExpression COLLATE?) { delegate.groupingValue( $COLLATE.text ); }
	;

havingClause
	:	{ delegate.activateHavingStrategy(); } ^(HAVING searchCondition) { delegate.deactivateStrategy(); }
	;

selectFrom
	:	^(SELECT_FROM fromClause selectClause)
	;

fromClause
	:	^(FROM persisterSpaces+)
	;

persisterSpaces
	:	^(PERSISTER_SPACE persisterSpace)
	;

persisterSpace
	:	persisterSpaceRoot joins*
	;

persisterSpaceRoot
	:	^(ENTITY_PERSISTER_REF entityName PROP_FETCH?)
	;

joins
	:	^(PROPERTY_JOIN jt=joinType ft=FETCH? an=ALIAS_NAME pf=PROP_FETCH?
		{ delegate.activateFromStrategy($jt.joinType, $ft, $pf, $an); }
		(collectionExpression|joinPropertyReference[$an]) withClause?)
		{ delegate.deactivateStrategy(); }
	|	^(PERSISTER_JOIN joinType persisterSpaceRoot onClause?)
	;

withClause
	:	^(WITH searchCondition)
	;

onClause
	:	^(ON searchCondition)
	;

joinType returns [JoinType joinType]
	:	CROSS { $joinType = JoinType.CROSS; }
	|	INNER { $joinType = JoinType.INNER; }
	|	(LEFT { $joinType = JoinType.LEFT; } | RIGHT { $joinType = JoinType.RIGHT; } | FULL { $joinType = JoinType.FULL; }) OUTER?
	;

selectClause
@init { if (state.backtracking == 0) delegate.activateSelectStrategy(); }
@after { delegate.deactivateStrategy(); }
	:	^(SELECT DISTINCT? rootSelectExpression)
	;

rootSelectExpression
	:	^(SELECT_LIST rootSelectExpression+)
	|	^(SELECT_ITEM rootSelectExpression)
	|	valueExpression ALIAS_NAME?
	;

orderByClause
	:	{ delegate.activateOrderByStrategy(); } ^(ORDER_BY sortSpecification+) { delegate.deactivateStrategy(); }
	;

sortSpecification
	:	^(SORT_SPEC valueExpression COLLATE? ORDER_SPEC) { delegate.sortSpecification( $COLLATE.text, $ORDER_SPEC.text.equals("asc") ); }
	;

searchCondition
	:	^( OR { delegate.activateOR(); } searchCondition searchCondition { delegate.deactivateBoolean(); } )
	|	^( AND { delegate.activateAND(); } searchCondition searchCondition { delegate.deactivateBoolean(); } )
	|	^( NOT { delegate.activateNOT(); } searchCondition { delegate.deactivateBoolean(); } )
	|	predicate
	;

predicate
   :  fullTextExpression
   |  TRUE { delegate.predicateConstantBoolean(true); }
   |  FALSE { delegate.predicateConstantBoolean(false); }
   |  ^( EQUALS rowValueConstructor comparativePredicateValue ) { delegate.predicateEquals( $comparativePredicateValue.text); }
	|	^( NOT_EQUAL rowValueConstructor comparativePredicateValue ) { delegate.predicateNotEquals( $comparativePredicateValue.text); }
	|	^( LESS rowValueConstructor comparativePredicateValue ) { delegate.predicateLess( $comparativePredicateValue.text); }
	|	^( LESS_EQUAL rowValueConstructor comparativePredicateValue ) { delegate.predicateLessOrEqual( $comparativePredicateValue.text); }
	|	^( GREATER rowValueConstructor comparativePredicateValue ) { delegate.predicateGreater( $comparativePredicateValue.text); }
	|	^( GREATER_EQUAL rowValueConstructor comparativePredicateValue ) { delegate.predicateGreaterOrEqual( $comparativePredicateValue.text); }
	|	^( IS_NULL rowValueConstructor ) { delegate.predicateIsNull(); }
	|	^( LIKE valueExpression patternValue=valueExpression escapeSpecification? ) { delegate.predicateLike( $patternValue.text, $escapeSpecification.escapeCharacter ); }
	|	^( BETWEEN rowValueConstructor betweenList )
	|	^( IN rowValueConstructor inPredicateValue ) { delegate.predicateIn( $inPredicateValue.elements ); }
	|	^( MEMBER_OF rowValueConstructor rowValueConstructor )
	|	^( NOT_MEMBER_OF rowValueConstructor rowValueConstructor  )
	|	^( IS_EMPTY rowValueConstructor )
	|	^( IS_NOT_EMPTY rowValueConstructor )
	|	rowValueConstructor
	;

betweenList
	:	^( BETWEEN_LIST lower=rowValueConstructor upper=rowValueConstructor ) { delegate.predicateBetween( $lower.text, $upper.text ); }
	;

comparativePredicateValue
	:	rowValueConstructor
	;

rowValueConstructor
	:	valueExpression
	;

escapeSpecification returns [Character escapeCharacter]
	:	^(ESCAPE characterValueExpression) { $escapeCharacter = $characterValueExpression.text.charAt( 0 ); }
	;

inPredicateValue returns [List<String> elements]
@init { $elements = new ArrayList<String>(); }
	:	^(IN_LIST (valueExpression { $elements.add($valueExpression.text); })+)
	;

numericValueExpression
	:	valueExpression
	;

characterValueExpression
	:	valueExpression
	;

datetimeValueExpression
	:	valueExpression
	;

valueExpression
   :  ^( MINUS numericValueExpression )
   |  ^( PLUS numericValueExpression )
	|	^( EXISTS rowValueConstructor)
	|	^( SOME valueExpression )
	|	^( ALL valueExpression )
	|	^( ANY valueExpression )
	|	^( VECTOR_EXPR valueExpression+) // or a tuples or ^(AND or IN statement
	|	valueExpressionPrimary
	;

valueExpressionPrimary
	:	function
	|	collectionExpression
	|	constant
	|	parameter
   |  propertyReferenceExpression
	|	ALIAS_REF //ID COLUMN, full property column list
	|	^(DOT_CLASS path) // crazy
	|	^(JAVA_CONSTANT path) //It will generate at SQL a parameter element (?) -> 'cos we do not need to care about char escaping
   ;

propertyReferenceExpression
   :  propertyReference
   |  ^(PATH propertyReferencePath) { delegate.setPropertyPath(((PropertyPathTree)$PATH ).getPropertyPath()); }
   ;

function
	:  geoFunction
	|  setFunction
	|  standardFunction
	;

geoFunction
@after { delegate.deactivateSpatial(); }
   :  ^(GEODIST { delegate.activateSpatial(SpatialFunction.GEODIST); } propertyReference? lat=signedNumericLiteralOrParameter lon=signedNumericLiteralOrParameter) { delegate.predicateGeodist($lat.text, $lon.text); }
   |  ^(GEOFILT { delegate.activateSpatial(SpatialFunction.GEOFILT); } propertyReference? lat=signedNumericLiteralOrParameter lon=signedNumericLiteralOrParameter dist=signedNumericLiteralOrParameter) { delegate.predicateGeofilt($lat.text, $lon.text, $dist.text); }
   ;

signedNumericLiteralOrParameter
    :  signedNumericLiteral
    |  parameter
    ;

setFunction
@after { delegate.deactivateAggregation(); }
	:	^(SUM { delegate.activateAggregation(AggregationFunction.SUM); } numericValueExpression)
	|	^(AVG { delegate.activateAggregation(AggregationFunction.AVG); } numericValueExpression)
	|	^(MAX { delegate.activateAggregation(AggregationFunction.MAX); } numericValueExpression)
	|	^(MIN { delegate.activateAggregation(AggregationFunction.MIN); } numericValueExpression)
	|	^(COUNT (ASTERISK { delegate.activateAggregation(AggregationFunction.COUNT); } | (DISTINCT { delegate.activateAggregation(AggregationFunction.COUNT_DISTINCT); } | ALL { delegate.activateAggregation(AggregationFunction.COUNT); }) countFunctionArguments))
	;

standardFunction
	:	sizeFunction
	|	indexFunction
	;

sizeFunction
	:	^(SIZE propertyReference)
	;

indexFunction
	:	^(INDEX ALIAS_REF)
	;

countFunctionArguments
	:	collectionExpression
	|	propertyReference
	|	numeric_literal
	;

collectionExpression
	:	^(ELEMENTS propertyReference) //it will generate a SELECT m.column form Table xxx -> it is realted to Hibernate mappings to Table->Map
	|	^(INDICES propertyReference)
	;

parameter
	:	NAMED_PARAM
	|	POSITIONAL_PARAM
	|	PARAM
	;

constant
   :  literal
   |  NULL
   |  TRUE
   |  FALSE
   ;

literal
	:	numeric_literal
	|	HEX_LITERAL
	|	OCTAL_LITERAL
	|	CHARACTER_LITERAL
	|	STRING_LITERAL
	|	^(CONST_STRING_VALUE CHARACTER_LITERAL)
	|	^(CONST_STRING_VALUE STRING_LITERAL)
	;

numeric_literal
	:	INTEGER_LITERAL
	|	DECIMAL_LITERAL
	|	FLOATING_POINT_LITERAL
	;

signedNumericLiteral
   :  ^(MINUS numeric_literal)
   |  ^(PLUS numeric_literal)
   |  numeric_literal
   ;

entityName
   :  ENTITY_NAME ALIAS_NAME { delegate.registerPersisterSpace(((EntityNameTree)$ENTITY_NAME).getEntityName(), $ALIAS_NAME); }
   ;

propertyReference
	:	^(PROPERTY_REFERENCE propertyReferencePath) { delegate.setPropertyPath( ( (PropertyPathTree)$PROPERTY_REFERENCE ).getPropertyPath() ); }
	;

joinPropertyReference[Tree alias]
	: ^(PATH propertyReferencePath) { delegate.registerJoinAlias( $alias, ( (PropertyPathTree) $PATH ).getPropertyPath() ); }
	;

propertyReferencePath
	:  {delegate.isUnqualifiedPropertyReference()}? unqualifiedPropertyReference
	|	pathedPropertyReference
	|	terminalIndexOperation
	;

unqualifiedPropertyReference
	:	IDENTIFIER
	;

pathedPropertyReference
	:	^(DOT pathedPropertyReferenceSource IDENTIFIER)
	;

pathedPropertyReferenceSource
	:	{delegate.isPersisterReferenceAlias()}?=> IDENTIFIER
	|	{delegate.isUnqualifiedPropertyReference()}?=> IDENTIFIER
	|	intermediatePathedPropertyReference
	|	intermediateIndexOperation
	;

intermediatePathedPropertyReference
	:	^(DOT source=pathedPropertyReferenceSource IDENTIFIER )
	;

intermediateIndexOperation
	:	^( LSQUARE indexOperationSource indexSelector )
	;

terminalIndexOperation
	:	^( LSQUARE indexOperationSource indexSelector )
	;

indexOperationSource
	:	^(DOT pathedPropertyReferenceSource IDENTIFIER )
	|	{delegate.isUnqualifiedPropertyReference()}?=> IDENTIFIER
	;

indexSelector
	:	valueExpression
	;

path
	: 	IDENTIFIER
	|	^(DOT path path)
	|	^(LSQUARE path valueExpression*)
	|	^(LPAREN path valueExpression*)
	;

fullTextExpression
   :  ^(COLON propertyReferenceExpression ftClause)
   ;

ftClause
   :  ^(FT_TERM ftLiteralOrParameter fuzzyFlop=TILDE?)
         { delegate.predicateFullTextTerm($ftLiteralOrParameter.text, $fuzzyFlop.text); }
   |  ^(FT_REGEXP REGEXP_LITERAL)
         { delegate.predicateFullTextRegexp($REGEXP_LITERAL.text); }
   |  ^(FT_RANGE startRange=(LSQUARE | LCURLY) lower=ftRangeBound upper=ftRangeBound endRange=(RSQUARE | RCURLY))
         { delegate.predicateFullTextRange($startRange.type == LSQUARE,
                                     $lower.start.getType() == ASTERISK ? null : $lower.text,
                                     $upper.start.getType() == ASTERISK ? null : $upper.text,
                                     $endRange.type == RSQUARE); }
   |  ^(OR { delegate.activateOR(); } ftClause ftClause { delegate.deactivateBoolean(); })
   |  ^(AND { delegate.activateAND(); } ftClause ftClause { delegate.deactivateBoolean(); })
   |  ^(CARAT { delegate.activateFullTextBoost(Float.parseFloat($CARAT.text)); } ftClause { delegate.deactivateFullTextBoost(); })
   |  ^(FT_OCCUR_SHOULD { delegate.activateFullTextOccur(QueryRendererDelegate.Occur.SHOULD); } ftClause { delegate.deactivateFullTextOccur(); })
   |  ^(FT_OCCUR_MUST { delegate.activateFullTextOccur(QueryRendererDelegate.Occur.MUST); } ftClause { delegate.deactivateFullTextOccur(); })
   |  ^(FT_OCCUR_MUST_NOT { delegate.activateFullTextOccur(QueryRendererDelegate.Occur.MUST_NOT); } ftClause { delegate.deactivateFullTextOccur(); })
   |  ^(FT_OCCUR_FILTER { delegate.activateFullTextOccur(QueryRendererDelegate.Occur.FILTER); } ftClause { delegate.deactivateFullTextOccur(); })
   ;

ftRangeBound
   :  ASTERISK
   |  ftLiteralOrParameter
   ;

ftLiteralOrParameter
   :  NAMED_PARAM
   |  ^(CONST_STRING_VALUE CHARACTER_LITERAL)
   |  ^(CONST_STRING_VALUE STRING_LITERAL)
   |  ^(MINUS numericLiteral)
   |  ^(PLUS numericLiteral)
   |  numericLiteral
   ;

numericLiteral
   :  INTEGER_LITERAL
   |  DECIMAL_LITERAL
   |  FLOATING_POINT_LITERAL
   |  HEX_LITERAL
   |  OCTAL_LITERAL
   ;
