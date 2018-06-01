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

tree grammar QueryResolver;

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
  private QueryResolverDelegate delegate;

  public QueryResolver(TreeNodeStream input, QueryResolverDelegate delegate) {
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
	:	^(WHERE searchCondition)
	;

groupByClause
	:	^(GROUP_BY groupingValue+)
	;

groupingValue
	:	^(GROUPING_VALUE valueExpression COLLATE?)
	;

havingClause
	:	^(HAVING searchCondition)
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
	:	^(ORDER_BY sortSpecification+)
	;

sortSpecification
	:	^(SORT_SPEC valueExpression COLLATE? ORDER_SPEC)
	;

searchCondition
	:	^( OR searchCondition searchCondition )
	|	^( AND searchCondition searchCondition )
	|	^( NOT searchCondition )
	|	predicate
	;

predicate
   :  fullTextExpression
   |  ^( EQUALS rowValueConstructor comparativePredicateValue )
	|	^( NOT_EQUAL rowValueConstructor comparativePredicateValue )
	|	^( LESS rowValueConstructor comparativePredicateValue )
	|	^( LESS_EQUAL rowValueConstructor comparativePredicateValue )
	|	^( GREATER rowValueConstructor comparativePredicateValue )
	|	^( GREATER_EQUAL rowValueConstructor comparativePredicateValue )
	|	^( IS_NULL rowValueConstructor )
	|	^( IS_NOT_NULL rowValueConstructor ) -> ^( NOT ^( IS_NULL rowValueConstructor ) )
	|	^( LIKE valueExpression valueExpression escapeSpecification? )
	|	^( NOT_LIKE valueExpression valueExpression escapeSpecification? ) -> ^( NOT ^( LIKE valueExpression valueExpression escapeSpecification? ) )
	|	^( BETWEEN rowValueConstructor betweenList )
	|	^( NOT_BETWEEN rowValueConstructor betweenList ) -> ^( NOT ^( BETWEEN rowValueConstructor betweenList ) )
	|	^( IN rowValueConstructor inPredicateValue )
	|	^( NOT_IN rowValueConstructor inPredicateValue ) -> ^( NOT ^( IN rowValueConstructor inPredicateValue ) )
	|	^( MEMBER_OF rowValueConstructor rowValueConstructor )
	|	^( NOT_MEMBER_OF rowValueConstructor rowValueConstructor  )
	|	^( IS_EMPTY rowValueConstructor )
	|	^( IS_NOT_EMPTY rowValueConstructor )
	|	rowValueConstructor
	;

betweenList
	:	^( BETWEEN_LIST rowValueConstructor rowValueConstructor )
	;

comparativePredicateValue
	:	rowValueConstructor
	;

rowValueConstructor
	:	valueExpression
	;

escapeSpecification
	:	^(ESCAPE characterValueExpression)
	;

inPredicateValue
	:	^(IN_LIST valueExpression+)
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
   |  ^(PATH ret=propertyReferencePath) -> ^(PATH<node=PropertyPathTree>[$PATH, $ret.retPath] propertyReferencePath)
   ;

function
	:  geoFunction
	|  setFunction
	|  standardFunction
	;

geoFunction
   :  ^(GEODIST propertyReference? signedNumericLiteralOrParameter signedNumericLiteralOrParameter)
   |  ^(GEOFILT propertyReference? signedNumericLiteralOrParameter signedNumericLiteralOrParameter signedNumericLiteralOrParameter)
   ;

signedNumericLiteralOrParameter
    :  signedNumericLiteral
    |  parameter
    ;

setFunction
	:	^(SUM numericValueExpression)
	|	^(AVG numericValueExpression)
	|	^(MAX numericValueExpression)
	|	^(MIN numericValueExpression)
	|	^(COUNT (ASTERISK | (DISTINCT | ALL) countFunctionArguments))
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
	:	^(PROPERTY_REFERENCE ret=propertyReferencePath) -> ^(PROPERTY_REFERENCE<node=PropertyPathTree>[$PROPERTY_REFERENCE, $ret.retPath] propertyReferencePath)
	;

joinPropertyReference[Tree alias]
	@after { delegate.registerJoinAlias(alias, $ret.retPath); }
	: ^(PATH ret=propertyReferencePath) -> ^(PATH<node=PropertyPathTree>[$PATH, $ret.retPath] propertyReferencePath)
	;

propertyReferencePath returns [PropertyPath retPath]
scope {
   PropertyPath path;
}
@init {
   $propertyReferencePath::path = new PropertyPath();
}
@after { $retPath = $propertyReferencePath::path; delegate.propertyPathCompleted( $propertyReferencePath::path ); }
	:  {delegate.isUnqualifiedPropertyReference()}? unqualifiedPropertyReference
	|	pathedPropertyReference
	|	terminalIndexOperation
	;

unqualifiedPropertyReference returns [PropertyPath.PropertyReference propertyReferenceSource]
	@after { $propertyReferencePath::path.append( $propertyReferenceSource ); }
	:	IDENTIFIER
	{	$propertyReferenceSource = delegate.normalizeUnqualifiedPropertyReference( $IDENTIFIER ); }
	;

pathedPropertyReference
	:	^(DOT pathedPropertyReferenceSource IDENTIFIER)
	{
		$propertyReferencePath::path.append( delegate.normalizePropertyPathTerminus( $propertyReferencePath::path, $IDENTIFIER ) );
	}
	;

pathedPropertyReferenceSource returns [PropertyPath.PropertyReference propertyReferenceSource]
	@after { $propertyReferencePath::path.append( $propertyReferenceSource ); }
	:	{delegate.isPersisterReferenceAlias()}?=> IDENTIFIER { $propertyReferenceSource = delegate.normalizeQualifiedRoot( $IDENTIFIER ); }
	|	{delegate.isUnqualifiedPropertyReference()}?=> IDENTIFIER { $propertyReferenceSource = delegate.normalizeUnqualifiedRoot( $IDENTIFIER ); }
	|	intermediatePathedPropertyReference { $propertyReferenceSource = $intermediatePathedPropertyReference.propertyReferenceSource; }
	|	intermediateIndexOperation { $propertyReferenceSource = $intermediateIndexOperation.propertyReferenceSource; }
	;

intermediatePathedPropertyReference returns [PropertyPath.PropertyReference propertyReferenceSource]
	:	^(DOT source=pathedPropertyReferenceSource IDENTIFIER )
	{	$propertyReferenceSource = delegate.normalizePropertyPathIntermediary( $propertyReferencePath::path, $IDENTIFIER );	}
	;

intermediateIndexOperation returns [PropertyPath.PropertyReference propertyReferenceSource]
	:	^( LSQUARE indexOperationSource indexSelector )
	{	$propertyReferenceSource = delegate.normalizeIntermediateIndexOperation( $indexOperationSource.propertyReferenceSource, $indexOperationSource.collectionProperty, $indexSelector.tree );	}
	;

terminalIndexOperation
	:	^( LSQUARE indexOperationSource indexSelector )
	{delegate.normalizeTerminalIndexOperation($indexOperationSource.propertyReferenceSource, $indexOperationSource.collectionProperty, $indexSelector.tree);}
	;

indexOperationSource returns [PropertyPath.PropertyReference propertyReferenceSource, Tree collectionProperty]
	:	^(DOT pathedPropertyReferenceSource IDENTIFIER )
	{	$propertyReferenceSource = $pathedPropertyReferenceSource.propertyReferenceSource;
		$collectionProperty = $IDENTIFIER;	}
		|	{delegate.isUnqualifiedPropertyReference()}?=> IDENTIFIER
		{ $propertyReferenceSource = delegate.normalizeUnqualifiedPropertyReferenceSource($IDENTIFIER);
		  $collectionProperty = $IDENTIFIER; }
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
   :  ^(FT_TERM ftLiteralOrParameter TILDE?)
   |  ^(FT_REGEXP REGEXP_LITERAL)
   |  ^(FT_RANGE (LSQUARE | LCURLY) ftRangeBound ftRangeBound (RSQUARE | RCURLY))
   |  ^(OR ftClause ftClause)
   |  ^(AND ftClause ftClause)
   |  ^(CARAT ftClause)
   |  ^(FT_OCCUR_SHOULD ftClause)
   |  ^(FT_OCCUR_MUST ftClause)
   |  ^(FT_OCCUR_MUST_NOT ftClause)
   |  ^(FT_OCCUR_FILTER ftClause)
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
