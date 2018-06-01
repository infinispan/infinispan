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

parser grammar IckleParser;

options {
	tokenVocab=IckleLexer;
	output=AST;
	superClass=ParserBase;
}

@parser::header {
package org.infinispan.objectfilter.impl.ql.parse;

import java.util.ArrayList;
import java.util.List;
}

@parser::members {
   private Tree generatePersisterSpacesTree(List persisterSpaces) {
      List<Tree> persisterSpaceList = new ArrayList<>();
      for (Tree persistenceSpaceData : (List<Tree>) persisterSpaces) {
         if (persistenceSpaceData.getType() == PERSISTER_JOIN || persistenceSpaceData.getType() == PROPERTY_JOIN) {
            adaptor.addChild(persisterSpaceList.get(persisterSpaceList.size() - 1), persistenceSpaceData);
         } else {
            Tree persistenceSpaceTree = (Tree) adaptor.becomeRoot(adaptor.create(PERSISTER_SPACE, "PERSISTER_SPACE"), adaptor.nil());
            adaptor.addChild(persistenceSpaceTree, persistenceSpaceData);
            persisterSpaceList.add(persistenceSpaceTree);
         }
      }
      Tree resultTree = (Tree) adaptor.nil();
      for (Tree persistenceElement : persisterSpaceList) {
         adaptor.addChild(resultTree, persistenceElement);
      }
      return resultTree;
   }

   /**
    * Provides a tree representing the SELECT clause. Will be the given SELECT clause if it is not {@code null},
    * otherwise a clause will be derived from the given FROM clause and aliases.
    */
   private Tree generateSelectFromTree(Tree selectClause, Tree fromClause, List<String> aliasList) {
      Tree result = new CommonTree(new CommonToken(SELECT_FROM, "SELECT_FROM"));
      result.addChild(fromClause);
      Tree selectTree;
      if (selectClause == null && aliasList != null && aliasList.size() > 0) {
         selectTree = new CommonTree(new CommonToken(SELECT, "SELECT"));
         Tree selectList = new CommonTree(new CommonToken(SELECT_LIST, "SELECT_LIST"));
         for (String aliasName : aliasList) {
            Tree selectElement = new CommonTree(new CommonToken(SELECT_ITEM, "SELECT_ITEM"));
            Tree aliasElement = new CommonTree(new CommonToken(ALIAS_REF, aliasName));
            selectElement.addChild(aliasElement);
            selectList.addChild(selectElement);
         }
         selectTree.addChild(selectList);
      } else {
         selectTree = selectClause;
      }
      result.addChild(selectTree);
      return result;
   }
}

statement
@init { if (state.backtracking == 0) pushEnableParameterUsage(true); }
@after { popEnableParameterUsage(); }
   :  selectStatement EOF!
   ;

selectStatement
   :  querySpec orderByClause? -> ^(QUERY querySpec orderByClause?)
   ;

querySpec
   :  selectFrom whereClause? ( groupByClause havingClause? )? -> ^(QUERY_SPEC selectFrom whereClause? groupByClause? havingClause?)
   ;

groupByClause
@init { if (state.backtracking == 0) pushEnableParameterUsage(false); }
@after { popEnableParameterUsage(); }
   :  group_by_key^ groupingSpecification
   ;

havingClause
	:	having_key^ logicalExpression
	;

groupingSpecification
	:	groupingValue ( COMMA! groupingValue )*
	;

groupingValue
   :  additiveExpression collationSpecification? -> ^(GROUPING_VALUE additiveExpression collationSpecification?)
	;

whereClause
	:	where_key^ logicalExpression
	;

selectFrom
   :  sc=selectClause? fc=fromClause -> { generateSelectFromTree((Tree) $sc.tree, (Tree) $fc.tree, $fc.aliasList) }
   ;

fromClause returns [List aliasList]
scope {
	List<String> aliases;
}
@init { $fromClause::aliases = new ArrayList<>(); }
@after { $aliasList = $fromClause::aliases; }
   :  from_key^ persisterSpaces
   ;

persisterSpaces
   :  ps+=persisterSpace ( COMMA ps+=persisterSpace )* -> { generatePersisterSpacesTree($ps) }
   ;

persisterSpace
	:	persisterSpaceRoot ( qualifiedJoin | crossJoin )*
	;

crossJoin
	:	cross_key join_key mainEntityPersisterReference
		-> ^(PERSISTER_JOIN[$join_key.start, "persister-join"] cross_key mainEntityPersisterReference)
	;

qualifiedJoin
@init { boolean isEntityReference = false; boolean hasFetch = false; }
@after { if (!hasFetch) $fromClause::aliases.add(((Tree)$ac.tree).getText()); }
	:	nonCrossJoinType join_key (fetch_key {hasFetch = true;})? path ac=aliasClause[true]
   (on_key { isEntityReference = true; } logicalExpression
   | propertyFetch? withClause?)
	-> {isEntityReference}? ^(PERSISTER_JOIN[$join_key.start, "persister-join"] nonCrossJoinType ^(ENTITY_PERSISTER_REF ENTITY_NAME<EntityNameTree>[$path.start, $path.text, (Tree) $path.tree] aliasClause?) ^(on_key logicalExpression))
	-> ^(PROPERTY_JOIN[$join_key.start, "property-join"] nonCrossJoinType fetch_key? aliasClause? propertyFetch? ^(PATH path) withClause?)
	;

withClause
	:	with_key^ logicalExpression
	;

nonCrossJoinType
	:	inner_key
	|	outerJoinType outer_key?
	|	-> INNER
	;

outerJoinType
	:	left_key
	|	right_key
	|	full_key
	;

persisterSpaceRoot
options {
backtrack=true;
}	:	hibernateLegacySyntax
	|	jpaCollectionReference
	|	mainEntityPersisterReference
	;

mainEntityPersisterReference
@after	{ $fromClause::aliases.add(((Tree)$ac.tree).getText()); }
	:	entityName ac=aliasClause[true] propertyFetch?
		-> ^(ENTITY_PERSISTER_REF entityName aliasClause? propertyFetch?)
	;

propertyFetch
   :  fetch_key all_key properties_key -> PROP_FETCH[$fetch_key.start, "property-fetch"]
   ;

hibernateLegacySyntax returns [boolean isPropertyJoin]
@init {$isPropertyJoin = false;}
@after	{ $fromClause::aliases.add(((Tree)$ad.tree).getText()); }
	:	ad=aliasDeclaration in_key
	(collectionExpression {$isPropertyJoin = true;} -> ^(PROPERTY_JOIN INNER[$in_key.start, "inner legacy"] aliasDeclaration collectionExpression))
	;

jpaCollectionReference
@after	{ $fromClause::aliases.add(((Tree)$ac.tree).getText()); }
	:	in_key LPAREN propertyReference RPAREN ac=aliasClause[true]
		-> ^(PROPERTY_JOIN INNER[$in_key.start, "inner"] aliasClause? propertyReference)
	;

selectClause
	:	select_key^ distinct_key? rootSelectExpression
	;

rootSelectExpression
	:	jpaSelectObjectSyntax
	|	explicitSelectList
	;

explicitSelectList
   :   explicitSelectItem ( COMMA explicitSelectItem )* -> ^(SELECT_LIST explicitSelectItem+)
   ;

explicitSelectItem
	:	selectExpression
	;

selectExpression
@init { if (state.backtracking == 0) pushEnableParameterUsage(false); }
@after { popEnableParameterUsage(); }
//PARAMETERS CAN'T BE USED -> This verification should be scoped
	:	expression aliasClause[false]
		-> ^(SELECT_ITEM expression aliasClause?)
	;

aliasClause[boolean generateAlias]
options {
    k=2;
}	:	-> {$generateAlias}? ALIAS_NAME[buildUniqueImplicitAlias()]
		->
	|	aliasDeclaration
	|	as_key! aliasDeclaration
	;

aliasDeclaration
	:	IDENTIFIER -> ALIAS_NAME[$IDENTIFIER]
	;

aliasReference
	:	IDENTIFIER -> ALIAS_REF[$IDENTIFIER]
	;

jpaSelectObjectSyntax
	:	object_key LPAREN aliasReference RPAREN -> ^(SELECT_ITEM aliasReference)
	;

orderByClause
   :  order_by_key^ sortSpecification ( COMMA! sortSpecification )*
   ;

sortSpecification
	:  sortKey collationSpecification? orderingSpecification -> ^(SORT_SPEC sortKey collationSpecification? orderingSpecification)
	;

orderingSpecification
	:	ascending_key -> ORDER_SPEC[$ascending_key.start, "asc"]
	|	descending_key -> ORDER_SPEC[$descending_key.start, "desc"]
	|  -> ORDER_SPEC["asc"]
	;

sortKey
@init	{ if (state.backtracking == 0) pushEnableParameterUsage(false); }
@after { popEnableParameterUsage(); }
//PARAMETERS CAN'T BE USED -> This verification should be scoped
	:	additiveExpression
	;

collationSpecification
	:  collate_key collateName -> COLLATE[$collateName.start, $collateName.text]
   ;

collateName
	:	dotIdentifierPath
	;

logicalExpression
	:	expression
	;

expression
	:	logicalOrExpression
	;

logicalOrExpression
	:	logicalAndExpression ( or_key^ logicalAndExpression )*
	;

logicalAndExpression
	:	negatedExpression ( and_key^ negatedExpression )*
	;

negatedExpression
   :  not_key^ negatedExpression
   |  equalityExpression
   ;

equalityExpression
@init { boolean isNull = false; boolean isNegated = false; }
   :  (ftOccurrence? ftFieldPath COLON)=> fullTextExpression
   |  (relationalExpression -> relationalExpression)
      (is_key (not_key {isNegated = true;})? (NULL {isNull = true;} | empty_key)
         -> {isNull && isNegated}? ^(IS_NOT_NULL[$not_key.start, "is not null"] $equalityExpression)
         -> {isNull && !isNegated}? ^(IS_NULL[$NULL, "is null"] $equalityExpression)
         -> {!isNull && isNegated}? ^(IS_NOT_EMPTY $equalityExpression)
         -> ^(IS_EMPTY $equalityExpression)
      |  (op=EQUALS | op=NOT_EQUAL) relationalExpression -> ^($op $equalityExpression relationalExpression)
      )*
	;

relationalExpression
@init {boolean isNegated = false;}
	:	(additiveExpression -> additiveExpression)
	(
	(	( op=LESS | op=GREATER | op=LESS_EQUAL | op=GREATER_EQUAL ) additiveExpression
			-> ^($op $relationalExpression additiveExpression)
		)+
	|  (not_key {isNegated = true;} )?
		(	in_key inList
			-> {isNegated}? ^(NOT_IN[$not_key.start, "not in"] $relationalExpression inList)
			-> ^(in_key $relationalExpression inList)
		|	between_key betweenList
			-> {isNegated}? ^(NOT_BETWEEN[$not_key.start, "not between"] $relationalExpression betweenList)
			-> ^(between_key $relationalExpression betweenList)
		|	like_key additiveExpression likeEscape?
			-> {isNegated}? ^(NOT_LIKE[$not_key.start, "not like"] $relationalExpression additiveExpression likeEscape?)
			-> ^(like_key $relationalExpression additiveExpression likeEscape?)
		|	member_of_key path
			-> {isNegated}? ^(NOT_MEMBER_OF[$not_key.start, "not member of"] $relationalExpression ^(PATH path))
			-> ^(member_of_key $relationalExpression ^(PATH path))
		)
	)?
	;

likeEscape
	:  escape_key^ additiveExpression
	;

inList
	:  collectionExpression -> ^(IN_LIST collectionExpression)
	|  LPAREN additiveExpression (COMMA additiveExpression)* RPAREN -> ^(IN_LIST additiveExpression+)
	;

betweenList
	:  lower=additiveExpression and_key upper=additiveExpression -> ^(BETWEEN_LIST $lower $upper)
	;

additiveExpression
   :  quantifiedExpression
   |  standardFunction
   |  geoFunction
   |  setFunction
   |  collectionExpression
   |  atom
   ;

quantifiedExpression
   :  (some_key^ | exists_key^ | all_key^ | any_key^) (collectionExpression | aliasReference)
   ;

standardFunction
@init { if (state.backtracking == 0) pushEnableParameterUsage(true); }
@after { popEnableParameterUsage(); }
   :  sizeFunction
   |  indexFunction
   ;

sizeFunction
   :   size_key^ LPAREN! propertyReference RPAREN!
   ;

indexFunction
   :   index_key^ LPAREN! aliasReference RPAREN!
   ;

geoFunction
   :   ( geodist_key^ ) LPAREN! (propertyReference COMMA!)? signedNumericLiteralOrParameter COMMA! signedNumericLiteralOrParameter RPAREN!
   |   ( geofilt_key^ ) LPAREN! (propertyReference COMMA!)? signedNumericLiteralOrParameter COMMA! signedNumericLiteralOrParameter COMMA! signedNumericLiteralOrParameter RPAREN!
   ;

signedNumericLiteralOrParameter
   :   signedNumericLiteral
   |   parameterSpecification
   ;

setFunction
@init { boolean generateOmittedElement = true; if (state.backtracking == 0) pushEnableParameterUsage(true); }
@after { popEnableParameterUsage(); }
	:	( sum_key^ | avg_key^ | max_key^ | min_key^ ) LPAREN! additiveExpression RPAREN!
	|	count_key LPAREN ( ASTERISK {generateOmittedElement = false;} | ( ( (distinct_key | all_key) {generateOmittedElement = false;} )? countFunctionArguments ) ) RPAREN
		-> {generateOmittedElement}? ^(count_key ASTERISK? ALL countFunctionArguments?)
		-> ^(count_key ASTERISK? distinct_key? all_key? countFunctionArguments?)
	;

countFunctionArguments
	:	propertyReference
	|	collectionExpression
	|	signedNumericLiteral
	;

collectionExpression
   :   (elements_key^ | indices_key^) LPAREN! propertyReference RPAREN!
   ;

atom
   :  identPrimary -> ^(PATH identPrimary)
	    //TODO  if ends with:
	    //  .class -> class type
	    //  if contains "()" it is a function call
	    //  if it is constantReference (using context)
	    //  otherwise it will be a generic element to be resolved on the next phase (1st tree walker)
   |  constant
   |  parameterSpecification { if (!isParameterUsageEnabled()) throw new RecognitionException(input); }
	//validate using Scopes if it is enabled or not to use parameterSpecification.. if not generate an exception
   |  LPAREN! expressionOrVector RPAREN!
   ;

parameterSpecification
   :  COLON IDENTIFIER -> NAMED_PARAM[$IDENTIFIER]
   |  PARAM INTEGER_LITERAL -> POSITIONAL_PARAM[$INTEGER_LITERAL]
   |  PARAM
	;

expressionOrVector
@init {boolean isVectorExp = false;}
	:	expression (vectorExpr {isVectorExp = true;})?
		-> {isVectorExp}? ^(VECTOR_EXPR expression vectorExpr)
		-> expression
	;

vectorExpr
@init	{ if (state.backtracking == 0) pushEnableParameterUsage(true); }
@after	{ popEnableParameterUsage(); }
	:	COMMA! expression (COMMA! expression)*
	;

identPrimary
	: 	IDENTIFIER
		(	DOT^ IDENTIFIER
		|	LSQUARE^ expression RSQUARE!
		|	LSQUARE^ RSQUARE!
		|	LPAREN^ exprList RPAREN!
		)*
	;

exprList
@init { if (state.backtracking == 0) pushEnableParameterUsage(true); }
@after { popEnableParameterUsage(); }
	:	expression (COMMA! expression)*
	|
	;

constant
   :  booleanLiteral
   |  stringLiteral
   |  signedNumericLiteral
   |  NULL
   ;

stringLiteral
   :  CHARACTER_LITERAL -> ^(CONST_STRING_VALUE CHARACTER_LITERAL)
   |  STRING_LITERAL -> ^(CONST_STRING_VALUE STRING_LITERAL)
   ;

booleanLiteral
   :  TRUE
   |  FALSE
   ;

numericLiteral
   :  INTEGER_LITERAL
   |  DECIMAL_LITERAL
   |  FLOATING_POINT_LITERAL
   |  HEX_LITERAL
   |  OCTAL_LITERAL
   ;

signedNumericLiteral
   :  (MINUS | PLUS)^ numericLiteral
   |  numericLiteral
   ;

entityName
   :  dotIdentifierPath -> ENTITY_NAME<EntityNameTree>[$dotIdentifierPath.start, $dotIdentifierPath.text, (Tree) $dotIdentifierPath.tree]
      //here the polymorphic entities should be resolved... to:
      // 1. to place inside the ENTITY_NAME Token all its possible values, otherwise it would be much difficult to return to the place that should explit the sentence
   ;

propertyReference
	:	path -> ^(PROPERTY_REFERENCE path)
	;

dotIdentifierPath
	:	IDENTIFIER (DOT^ IDENTIFIER)*
	;

path
	:	IDENTIFIER
		(	DOT^ IDENTIFIER
		|	LSQUARE^ expression RSQUARE!
		|	LSQUARE^ RSQUARE!
		)*
	;

object_key
   :   {validateSoftKeyword("object")}?=> IDENTIFIER -> OBJECT[$IDENTIFIER]
	;

sum_key
   :   {validateSoftKeyword("sum")}?=> IDENTIFIER -> SUM[$IDENTIFIER]
	;

avg_key
   :   {validateSoftKeyword("avg")}?=> IDENTIFIER -> AVG[$IDENTIFIER]
	;

max_key
   :   {validateSoftKeyword("max")}?=> IDENTIFIER -> MAX[$IDENTIFIER]
	;

min_key
   :   {validateSoftKeyword("min")}?=> IDENTIFIER -> MIN[$IDENTIFIER]
	;

count_key
   :   {validateSoftKeyword("count")}?=> IDENTIFIER -> COUNT[$IDENTIFIER]
	;

size_key
   :   {validateSoftKeyword("size")}?=> IDENTIFIER -> SIZE[$IDENTIFIER]
	;

index_key
   :   {validateSoftKeyword("index")}?=> IDENTIFIER -> INDEX[$IDENTIFIER]
	;

any_key
   :   {validateSoftKeyword("any")}?=> IDENTIFIER -> ANY[$IDENTIFIER]
	;

exists_key
   :   {validateSoftKeyword("exists")}?=> IDENTIFIER -> EXISTS[$IDENTIFIER]
	;

some_key
   :   {validateSoftKeyword("some")}?=> IDENTIFIER -> SOME[$IDENTIFIER]
	;

escape_key
   :   {validateSoftKeyword("escape")}?=> IDENTIFIER -> ESCAPE[$IDENTIFIER]
	;

like_key
   :   {validateSoftKeyword("like")}?=> IDENTIFIER -> LIKE[$IDENTIFIER]
	;

between_key
   :   {validateSoftKeyword("between")}?=> IDENTIFIER -> BETWEEN[$IDENTIFIER]
	;

member_of_key
	:   {validateSoftKeyword("member") && validateSoftKeyword(2, "of")}?=> id=IDENTIFIER IDENTIFIER -> MEMBER_OF[$id]
	;

empty_key
   :   {validateSoftKeyword("empty")}?=> IDENTIFIER -> EMPTY[$IDENTIFIER]
	;

is_key
   :   {validateSoftKeyword("is")}?=> IDENTIFIER -> IS[$IDENTIFIER]
   ;

or_key
   :   OR
   |   {validateSoftKeyword("or")}?=> IDENTIFIER -> OR[$IDENTIFIER]
	;

and_key
   :   AND
   |   {validateSoftKeyword("and")}?=> IDENTIFIER -> AND[$IDENTIFIER]
	;

not_key
   :   EXCLAMATION
   |   {validateSoftKeyword("not")}?=> IDENTIFIER -> NOT[$IDENTIFIER]
   ;

to_key
   :   {validateSoftKeyword("to")}?=> IDENTIFIER -> TO[$IDENTIFIER]
   ;

having_key
   :   {validateSoftKeyword("having")}?=> IDENTIFIER -> HAVING[$IDENTIFIER]
	;

with_key
   :   {validateSoftKeyword("with")}?=> IDENTIFIER -> WITH[$IDENTIFIER]
	;

on_key
   :   {validateSoftKeyword("on")}?=> IDENTIFIER -> ON[$IDENTIFIER]
	;

indices_key
   :   {validateSoftKeyword("indices")}?=> IDENTIFIER -> INDICES[$IDENTIFIER]
	;

cross_key
   :   {validateSoftKeyword("cross")}?=> IDENTIFIER -> CROSS[$IDENTIFIER]
	;

join_key
   :   {validateSoftKeyword("join")}?=> IDENTIFIER -> JOIN[$IDENTIFIER]
	;

inner_key
   :   {validateSoftKeyword("inner")}?=> IDENTIFIER -> INNER[$IDENTIFIER]
	;

outer_key
   :   {validateSoftKeyword("outer")}?=> IDENTIFIER -> OUTER[$IDENTIFIER]
	;

left_key
   :   {validateSoftKeyword("left")}?=> IDENTIFIER -> LEFT[$IDENTIFIER]
	;

right_key
   :   {validateSoftKeyword("right")}?=> IDENTIFIER -> RIGHT[$IDENTIFIER]
	;

full_key
   :   {validateSoftKeyword("full")}?=> IDENTIFIER -> FULL[$IDENTIFIER]
	;

elements_key
   :   {validateSoftKeyword("elements")}?=> IDENTIFIER -> ELEMENTS[$IDENTIFIER]
	;

geofilt_key
   :   {validateSoftKeyword("geofilt")}?=> IDENTIFIER -> GEOFILT[$IDENTIFIER]
	;

geodist_key
   :   {validateSoftKeyword("geodist")}?=> IDENTIFIER -> GEODIST[$IDENTIFIER]
	;

properties_key
   :   {validateSoftKeyword("properties")}?=> IDENTIFIER -> PROPERTIES[$IDENTIFIER]
	;

fetch_key
   :   {validateSoftKeyword("fetch")}?=> IDENTIFIER -> FETCH[$IDENTIFIER]
	;

in_key
   :   {validateSoftKeyword("in")}?=> IDENTIFIER -> IN[$IDENTIFIER]
	;

as_key
   :   {validateSoftKeyword("as")}?=> IDENTIFIER -> AS[$IDENTIFIER]
	;

where_key
   :   {validateSoftKeyword("where")}?=> IDENTIFIER -> WHERE[$IDENTIFIER]
	;

select_key
   :   {validateSoftKeyword("select")}?=> IDENTIFIER -> SELECT[$IDENTIFIER]
	;

distinct_key
   :   {validateSoftKeyword("distinct")}?=> IDENTIFIER -> DISTINCT[$IDENTIFIER]
	;

all_key
   :   {validateSoftKeyword("all")}?=> IDENTIFIER -> ALL[$IDENTIFIER]
	;

ascending_key
   :   {validateSoftKeyword("asc")}?=> IDENTIFIER
	;

descending_key
   :   {validateSoftKeyword("desc")}?=> IDENTIFIER
	;

collate_key
   :   {validateSoftKeyword("collate")}?=> IDENTIFIER
	;

order_by_key
   :   {validateSoftKeyword("order") && validateSoftKeyword(2, "by")}?=> id=IDENTIFIER IDENTIFIER -> ORDER_BY[$id]
	;

group_by_key
   :   {validateSoftKeyword("group") && validateSoftKeyword(2, "by")}?=> id=IDENTIFIER IDENTIFIER -> GROUP_BY[$id]
	;

from_key
   :   {validateSoftKeyword("from")}?=> IDENTIFIER -> FROM[$IDENTIFIER]
	;

/* TODO [anistor]
DIFFERENCES TO LUCENE SYNTAX:
 - whitespace is not significant.
 - no wildcard (*) for field names, so *:* is not a valid query.
 - a field name/path is always specified. we do not have a default field like lucene has.
 - = operator is not accepted instead of : as in lucene StandardSyntaxParser
 - &&, || are accepted alternatives for AND/OR, in both full-text and jpa predicates
 - ! can be used instead of NOT
 - AND,OR,NOT are capitalised in lucene but in infinispan they are case insensitive
 - when and/or is missing, OR will be assumed
 - string terms must be enclosed in single or double quotes. lucene allows single-word terms to be unquoted
 - fuzziness and boosting are not accepted in arbitrary order as in lucene's parser. fuzziness must always come first.
 - we accept != instead of <> from jpa
 - >,>=,<,<= operators work as in JPA not as in lucene's StandardSyntaxParser so boosting cannot be applied. use ranges to achieve that.
*/

fullTextExpression
   :  ftOccurrence ftFieldPath COLON ftBoostedQuery -> ^(COLON ftFieldPath ^(ftOccurrence ftBoostedQuery))
   |  ftFieldPath COLON^ ftBoostedQuery
   ;

ftOccurrence
   :  PLUS -> FT_OCCUR_MUST[$PLUS, $PLUS.text]
   |  MINUS -> FT_OCCUR_MUST_NOT[$MINUS, $MINUS.text]
   |  EXCLAMATION -> FT_OCCUR_MUST_NOT[$EXCLAMATION, $EXCLAMATION.text]
   |  HASH -> FT_OCCUR_FILTER[$HASH, $HASH.text]
   |  not_key -> FT_OCCUR_MUST_NOT[$not_key.start, $not_key.text]
   ;

ftFieldPath
   :  dotIdentifierPath -> ^(PATH dotIdentifierPath)
   ;

ftBoostedQuery
   :  ftTermOrQuery ftBoost^?
   ;

// TODO [anistor] maybe we should not accept space between CARAT and following number/param to get rid of syntactic ambiguity
ftBoost
   :  CARAT ftNumericLiteralOrParameter -> CARAT[$CARAT, $ftNumericLiteralOrParameter.text]
   ;

ftTermOrQuery
   :  ftTerm
   |  ftRange
   |  LPAREN! ftConjunction (ftOr^ ftConjunction)* RPAREN!
   ;

ftTerm
   :  ftLiteralOrParameter ftFuzzySlop? -> ^(FT_TERM ftLiteralOrParameter ftFuzzySlop?)
   |  REGEXP_LITERAL -> ^(FT_REGEXP REGEXP_LITERAL)
   ;

// TODO [anistor] maybe we should not accept space between TILDE and following number/param to get rid of syntactic ambiguity
ftFuzzySlop
   :  TILDE (options { greedy=true; } : ftNumericLiteralOrParameter)? -> TILDE[$TILDE, $ftNumericLiteralOrParameter.text]
   ;

ftRange
   :  ftRangeBegin lower=ftRangeBound to_key? upper=ftRangeBound ftRangeEnd -> ^(FT_RANGE ftRangeBegin $lower $upper ftRangeEnd)
   ;

ftRangeBegin
   :  LSQUARE
   |  LCURLY
   ;

ftRangeEnd
   :  RSQUARE
   |  RCURLY
   ;

ftRangeBound
   :  ASTERISK
   |  ftLiteralOrParameter
   ;

ftOr
   :  or_key
   |  -> OR
   ;

ftConjunction
   :  ftClause (and_key^ ftClause)*
   ;

ftClause
   :  (options { greedy=true; } : ftOccurrence^)? ftBoostedQuery
   ;

ftLiteralOrParameter
   :  parameterSpecification
   |  stringLiteral
   |  signedNumericLiteral
   ;

ftNumericLiteralOrParameter
   :  parameterSpecification
   |  numericLiteral
   ;
