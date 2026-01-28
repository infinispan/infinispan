parser grammar IckleParser;

options {
	tokenVocab=IckleLexer;
}

@parser::header {
/*
 * SPDX-License-Identifier: Apache-2.0
 */
import java.util.ArrayList;
import java.util.List;
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// Statements

/**
 * Toplevel rule, entrypoint to the whole grammar
 */
statement
  : (selectStatement | deleteStatement) EOF
  ;

/**
 * A 'select' query
 */
selectStatement
   :  selectFrom whereClause? ( groupByClause havingClause? )? orderByClause?
   ;

/**
 * A 'delete' statement
 */
deleteStatement
   :  deleteClause whereClause?
   ;

deleteClause
   :  delete_key fromClause
   ;

querySpec
   :  selectFrom whereClause? ( groupByClause havingClause? )?  (QUERY_SPEC selectFrom whereClause? groupByClause? havingClause?)
   ;

groupByClause
  : group_by_key groupingSpecification
  ;

havingClause
	:  having_key logicalExpression
	;

groupingSpecification
	:	groupingValue ( COMMA groupingValue )*
	;

groupingValue
   :  additiveExpression collationSpecification?
	;

filteringClause
   :  filtering_key logicalExpression
   ;

whereClause
	:	where_key logicalExpression
	;

selectFrom
    : sc=selectClause? fc=fromClause
    ;

fromClause
    : from_key persisterSpaces
    ;

persisterSpaces
    : ps+=persisterSpace ( COMMA ps+=persisterSpace )*
    ;

persisterSpace
	:	persisterSpaceRoot ( qualifiedJoin | crossJoin )*
	;

crossJoin
	:	cross_key join_key mainEntityPersisterReference
		 (PERSISTER_JOIN cross_key mainEntityPersisterReference)
	;

qualifiedJoin
    : nonCrossJoinType? join_key fetch_key? path aliasClause?
      (on_key logicalExpression | propertyFetch? withClause?)?
    ;

withClause
	:	with_key logicalExpression
	;

nonCrossJoinType
	:	inner_key
	|	outerJoinType outer_key?
	|	 INNER
	;

outerJoinType
	:	left_key
	|	right_key
	|	full_key
	;

persisterSpaceRoot
	:	mainEntityPersisterReference
	|	hibernateLegacySyntax
	|	jpaCollectionReference
	;

mainEntityPersisterReference
    :  entityName aliasClause? propertyFetch?
    ;

propertyFetch
   :  fetch_key all_key properties_key
   ;

hibernateLegacySyntax
    : aliasDeclaration in_key
      (collectionExpression
      (PROPERTY_JOIN INNER aliasDeclaration collectionExpression)?)?
    ;

jpaCollectionReference
    : in_key LPAREN propertyReference RPAREN ac=aliasClause
      (PROPERTY_JOIN INNER aliasClause? propertyReference)?
    ;

selectClause
	:	select_key distinct_key? rootSelectExpression
	;

rootSelectExpression
	:	jpaSelectObjectSyntax
	|	explicitSelectList
	;

explicitSelectList
   :   explicitSelectItem ( COMMA explicitSelectItem )*
   ;

explicitSelectItem
	:	selectExpression
	;

selectExpression
    : expression aliasClause?
    ;

identifier
   : IDENTIFIER
   | ALL
   | ANY
   | AS
   | ASC
   | AVG
   | BETWEEN
   | BOUNDINGBOX
   | CIRCLE
   | COUNT
   | CROSS
   | DISTANCE
   | DISTINCT
   | DELETE
   | DESC
   | ELEMENTS
   | EMPTY
   | ESCAPE
   | EXISTS
   | FETCH
   | FILTERING
   | FROM
   | FULL
   | GROUP
   | HAVING
   | IN
   | INDEX
   | INDICES
   | INNER
   | IS
   | JOIN
   | KILOMETERS
   | LEFT
   | LIKE
   | MAX
   | MEMBER
   | METERS
   | MILES
   | MIN
   | NAUTICAL_MILES
   | NOT
   | OBJECT
   | OF
   | ON
   | ORDER
   | OUTER
   | POLYGON
   | PROPERTIES
   | RIGHT
   | SCORE
   | SELECT
   | SIZE
   | SUM
   | SOME
   | VERSION
   | WHERE
   | WITH
   | WITHIN
   | YARDS
   | BY
   | TO
   | TRUE
   | FALSE
   | NULL
   | ASC
   | DESC
   | SET
   | VERSIONED
   | KEY
   | VALUE
   | ENTRY
   | TYPE
   ;

aliasClause
    : as_key aliasDeclaration
    | aliasDeclaration
    ;

aliasDeclaration
	:	AS? aliasIdentifier
	;

aliasIdentifier
    : identifier
    | avg_key
    | count_key
    | sum_key
    | min_key
    | max_key
    ;

aliasReference
	:	identifier
	;

jpaSelectObjectSyntax
	:	object_key LPAREN aliasReference RPAREN
	;

orderByClause
   :  order_by_key sortSpecification ( COMMA sortSpecification )*
   ;

sortSpecification
	:  sortKey collationSpecification? orderingSpecification?
	;

orderingSpecification
   returns [String order]
   : ascending_key  { $order = "asc"; }
   | descending_key { $order = "desc"; }
   ;

sortKey
	:	additiveExpression
	;

collationSpecification
    returns [String collation]
  : COLLATE_KEY n=collateName { $collation = $n.text; }
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
	:	logicalAndExpression ( or_key logicalAndExpression )*
	;

logicalAndExpression
	:	negatedExpression ( and_key negatedExpression )*
	;

negatedExpression
   :  not_key negatedExpression
   |  equalityExpression
   ;

equalityExpression
    locals [boolean isNull = false, boolean isNegated = false]
    : ftOccurrence fullTextExpression
    | fullTextExpression
    | knnExpression
    | relationalExpression equalityTail*
    ;

// null/empty
equalityTail
    : isClause
    | comparisonClause
    ;

// IS NULL, IS NOT NULL, IS EMPTY, IS NOT EMPTY
isClause
    : is_key not_key?
      (NULL | empty_key)
    ;

// (=, !=)
comparisonClause
    : op=(EQUALS | NOT_EQUAL) relationalExpression
    ;

relationalExpression
locals [boolean isNegated = false]
    : additiveExpression relationalTail*
    ;

relationalTail
    : op=(LESS | GREATER | LESS_EQUAL | GREATER_EQUAL) additiveExpression
    | not_key? relationalOperator
    ;

relationalOperator
    : in_key inList
    | between_key betweenList
    | like_key additiveExpression likeEscape?
    | member_of_key path
    | within_key geoShape
    ;

geoShape
    : geoCircle
    | geoBoundingBox
    | geoPolygon
    ;

geoCircle
    : circle_key
      LPAREN
      latAtom=atom COMMA
      lonAtom=atom COMMA
      radiusAtom=distanceVal
      RPAREN
    ;

geoBoundingBox
    : boundingBox_key
      LPAREN
      tlLatAtom=atom COMMA
      tlLonAtom=atom COMMA
      brLatAtom=atom COMMA
      brLonAtom=atom
      RPAREN
    ;

geoPolygon
    : polygon_key LPAREN geoPolygonArg (COMMA geoPolygonArg)* RPAREN
    ;

geoPolygonArg
    : LPAREN atom COMMA atom RPAREN
    | parameterSpecification
    ;

geoPoint
    : LPAREN lat=atom COMMA lon=atom RPAREN
    ;

likeEscape
	:  escape_key additiveExpression
	;

inList
   :  collectionExpression
   |  LPAREN additiveExpression (COMMA additiveExpression)* RPAREN
   ;

betweenList
    returns [Object lower, Object upper]
    : lowerExpr=additiveExpression
      and_key
      upperExpr=additiveExpression
    ;


additiveExpression
   :  quantifiedExpression
   |  standardFunction
   |  distanceFunction
   |  setFunction
   |  versionFunction
   |  scoreFunction
   |  collectionExpression
   |  atom
   ;

distanceFunction
   : distance_key LPAREN propertyReference COMMA lat=atom COMMA lon=atom distanceFunctionUnit? RPAREN
   ;

distanceFunctionUnit
   : COMMA unit=unitVal
   ;

quantifiedExpression
   :  (some_key | exists_key | all_key | any_key) (collectionExpression | aliasReference)
   ;

standardFunction
   :  sizeFunction
   |  indexFunction
   ;

sizeFunction
   :   size_key LPAREN propertyReference RPAREN
   ;

indexFunction
   :   index_key LPAREN aliasReference RPAREN
   ;

versionFunction
   :   version_key LPAREN aliasReference RPAREN
   ;

scoreFunction
   :   score_key LPAREN aliasReference RPAREN
   ;

setFunction
  : simpleAggFunction
  | countFunction
  ;

simpleAggFunction
  : (sum_key | avg_key | max_key | min_key) LPAREN additiveExpression RPAREN
  ;

countFunction
  : count_key LPAREN countArg RPAREN
  ;

countArg
  : ASTERISK
  | ( (distinct_key | all_key)? countFunctionArguments )
  ;

countFunctionArguments
	:	propertyReference
	|	collectionExpression
	|	signedNumericLiteral
	;

collectionExpression
   :   (elements_key | indices_key) LPAREN propertyReference RPAREN
   ;

vectorSearch
   : LSQUARE expression (COMMA expression)* RSQUARE
   ;

atom
   :  identPrimary
	    //TODO  if ends with:
	    //  .class  class type
	    //  if contains "()" it is a function call
	    //  if it is constantReference (using context)
	    //  otherwise it will be a generic element to be resolved on the next phase (1st tree walker)
   |  constant
   |  LPAREN expressionOrVector RPAREN
   |  vectorSearch
   ;

distanceVal
   :  constant unit=unitVal?
   ;

unitVal
   : meters_key
   | kilometers_key
   | miles_key
   | yards_key
   | nautical_miles_key
   ;

parameterSpecification
    : COLON id=anyIdentifier
    | PARAM intVal=INTEGER_LITERAL
    | PARAM
    ;

anyIdentifier
    : identifier
    | DISTANCE | CIRCLE | WITHIN | METERS | KILOMETERS
    ;

expressionOrVector
  : vectorExpression
  | expression
  ;

vectorExpression
  : expression (COMMA expression)+
  ;

vectorExpr
	:	COMMA expression (COMMA expression)*
	;

identPrimary
	: 	identifier
		(	DOT identifier
		|	LSQUARE expression RSQUARE
		|	LSQUARE RSQUARE
		|	LPAREN exprList RPAREN
		)*
	;

exprList
	:	expression (COMMA expression)*
	|
	;

constant
   :  parameterSpecification
   |  booleanLiteral
   |  stringLiteral
   |  signedNumericLiteral
   |  NULL
   ;

stringLiteral
   :  CHARACTER_LITERAL
   |  STRING_LITERAL
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
   :  (MINUS | PLUS) numericLiteral
   |  numericLiteral
   ;

entityName
    : dip=dotIdentifierPath
    ;

propertyReference
	:	path
	;

dotIdentifierPath
	:	identifier (DOT identifier)*
	;

path
	:	identifier
		(	DOT identifier
		|	LSQUARE expression RSQUARE
		|	LSQUARE RSQUARE
		)*
	;

object_key
   :   OBJECT
	;

sum_key
   :   SUM
	;

avg_key
   :   AVG
	;

max_key
   :   MAX
	;

min_key
   :   MIN
	;

count_key
   :   COUNT
	;

version_key
   :   VERSION
	;

score_key
   :   SCORE
	;

size_key
   :   SIZE
	;

index_key
   :   INDEX
	;

any_key
   :   ANY
	;

exists_key
   :   EXISTS
	;

some_key
   :   SOME
	;

escape_key
   :   ESCAPE
	;

like_key
   :   LIKE
	;

between_key
   :   BETWEEN
	;

member_of_key
	:   MEMBER OF
	;

empty_key
   :   EMPTY
	;

is_key
   :   IS
   ;

or_key
   :   OR
   |   OR_KEYWORD
	;

and_key
   :   AND
   |   AND_KEYWORD
	;

not_key
   :   EXCLAMATION
   |   NOT
   ;

to_key
   :   TO
   ;

having_key
   :   HAVING
	;

filtering_key
   :   FILTERING
	;

with_key
   :   WITH
	;

within_key
   :   WITHIN
   ;

distance_key
   :   DISTANCE
   ;

circle_key
   :   CIRCLE
   ;

boundingBox_key
   :   BOUNDINGBOX
   ;

polygon_key
   :   POLYGON
   ;

on_key
   :   ON
	;

meters_key:      METERS | {_input.LT(1).getText().equals("m")}? identifier;
kilometers_key:  KILOMETERS | {_input.LT(1).getText().equals("km")}? identifier;
miles_key:       MILES | {_input.LT(1).getText().equals("mi")}? identifier;
yards_key:       YARDS | {_input.LT(1).getText().equals("yd")}? identifier;
nautical_miles_key: NAUTICAL_MILES
   | {_input.LT(1).getText().equals("nm")}? identifier
   | {_input.LT(1).getText().equals("nmi")}? identifier;

indices_key
   :   INDICES
	;

cross_key
   :   CROSS
	;

join_key
   :   JOIN
	;

inner_key
   :   INNER
	;

outer_key
   :   OUTER
	;

left_key
   :   LEFT
	;

right_key
   :   RIGHT
	;

full_key
   :   FULL
	;

elements_key
   :   ELEMENTS
	;

properties_key
   :   PROPERTIES
	;

fetch_key
   :   FETCH
	;

in_key
   :   IN
	;

as_key
   :   AS
	;

where_key
   :   WHERE
	;

select_key
   :   SELECT
	;

delete_key
   :   DELETE
   ;

distinct_key
   :   DISTINCT
	;

all_key
   :   ALL
	;

ascending_key
   :   ASC
	;

descending_key
   :   DESC
	;

collate_key
   :   identifier
	;

order_by_key
   :   id=ORDER BY
	;

group_by_key
   :   id=GROUP BY
	;

from_key
   :   FROM
	;

/*
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
   :  ftFieldPath COLON ftBoostedQuery
   ;

knnExpression
   : vectorFieldPath ARROW knnTerm
   ;

ftOccurrence
    : plus=PLUS
    | minus=MINUS
    | excl=EXCLAMATION
    | hash=HASH
    | nk=not_key
    ;

ftFieldPath
   :  dotIdentifierPath
   ;

vectorFieldPath
   :  dotIdentifierPath
   ;

ftBoostedQuery
   :  ftTermOrQuery ftBoost?
   ;

ftBoost
    : c1=CARAT val=ftNumericLiteralOrParameter
    ;

ftTermOrQuery
    : ftTerm
    | ftRange
    | LPAREN ftConjunction (ftOr ftConjunction)* RPAREN
    ;

ftTerm
   :  ftLiteralOrParameter ftFuzzySlop?
   |  REGEXP_LITERAL
   ;

knnTerm
   :  vectorSearch knnDistance? filteringClause?
   ;

ftFuzzySlop
    : TILDE ftNumericLiteralOrParameter?
    ;

ftRange
    : rb=ftRangeBegin lower=ftRangeBound tk=to_key? upper=ftRangeBound re=ftRangeEnd
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
   |   OR
   ;

ftConjunction
   :  ftClause (ftAnd? ftClause)*
   ;

ftAnd
   :  and_key
   |  AND
   ;

ftClause
   :  ftOccurrence? ftBoostedQuery
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

knnDistance
    : TILDE val=ftNumericLiteralOrParameter?
    ;

nakedIdentifier
   :identifier
   | JOIN
   | INDEX
   | VERSION
   | SOME
   | SCORE
   | SIZE
   | INDEX
   | ANY
   | COUNT
   | MIN
   ;