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

lexer grammar IckleLexer;

tokens {
//VIRTUAL TOKENS
  ALIAS_NAME;
  ALIAS_REF;
  BETWEEN_LIST;
  COLLATE;
  COLLECTION_EXPRESSION;
  DOT_CLASS;
  ENTITY_NAME;
  ENTITY_PERSISTER_REF;
  GROUPING_VALUE;
  IN_LIST;
  IS_NOT_EMPTY;
  IS_NOT_NULL;
  IS_NULL;
  JAVA_CONSTANT;
  POSITIONAL_PARAM;
  NAMED_PARAM;
  NOT_BETWEEN;
  NOT_IN;
  NOT_LIKE;
  NOT_MEMBER_OF;
  ORDER_SPEC;
  PATH;
  PERSISTER_JOIN;
  PERSISTER_SPACE;
  PROP_FETCH;
  PROPERTY_JOIN;
  PROPERTY_REFERENCE;
  QUALIFIED_JOIN;
  QUERY_SPEC;
  QUERY;
  SELECT_FROM;
  SELECT_ITEM;
  SELECT_LIST;
  SORT_SPEC;
  VECTOR_EXPR;
  CONST_STRING_VALUE;
  FT_OCCUR_MUST;
  FT_OCCUR_FILTER;
  FT_OCCUR_SHOULD;
  FT_OCCUR_MUST_NOT;
  FT_TERM;
  FT_REGEXP;
  FT_RANGE;

//SOFT KEYWORDS
  ALL;
  AND;
  ANY;
  AS;
  AVG;
  BETWEEN;
  COUNT;
  CROSS;
  DISTINCT;
  ELEMENTS;
  EMPTY;
  ESCAPE;
  EXISTS;
  FETCH;
  FROM;
  FULL;
  GEODIST;
  GEOFILT;
  GROUP_BY;
  HAVING;
  IN;
  INDEX;
  INDICES;
  INNER;
  IS_EMPTY;
  IS;
  JOIN;
  LEFT;
  LIKE;
  MAX;
  MEMBER_OF;
  MIN;
  NOT;
  OBJECT;
  ON;
  OR;
  ORDER_BY;
  OUTER;
  PROPERTIES;
  RIGHT;
  SELECT;
  SIZE;
  SOME;
  SUM;
  WHERE;
  WITH;
}

@header {
package org.infinispan.objectfilter.impl.ql.parse;
}

WS: (' ' | '\t' | '\v' | '\f' | EOL)+ { $channel = HIDDEN; };

fragment NL: ('\r' | '\n') ;

fragment EOL: NL+ ;

HEX_LITERAL: '0' ('x'|'X') HEX_DIGIT+ INTEGER_TYPE_SUFFIX? ;

INTEGER_LITERAL: ('0' | '1'..'9' '0'..'9'*) ;

DECIMAL_LITERAL: ('0' | '1'..'9' '0'..'9'*) INTEGER_TYPE_SUFFIX ;

OCTAL_LITERAL: '0' ('0'..'7')+ INTEGER_TYPE_SUFFIX? ;

fragment HEX_DIGIT: ('0'..'9' | 'a'..'f' | 'A'..'F') ;

fragment INTEGER_TYPE_SUFFIX: ('l'|'L') ;

FLOATING_POINT_LITERAL:
  ('0'..'9')+ '.' ('0'..'9')* EXPONENT? FLOAT_TYPE_SUFFIX?
  |  '.' ('0'..'9')+ EXPONENT? FLOAT_TYPE_SUFFIX?
  |  ('0'..'9')+ EXPONENT FLOAT_TYPE_SUFFIX?
  |  ('0'..'9')+ FLOAT_TYPE_SUFFIX
  ;

fragment EXPONENT: ('e'|'E') ('+'|'-')? ('0'..'9')+ ;

fragment FLOAT_TYPE_SUFFIX: ('f'|'F') | ('d'|'D') ;

CHARACTER_LITERAL:
  '\'' ( ESCAPE_SEQUENCE | ~('\''|'\\') ) '\'' { setText(getText().substring(1, getText().length() - 1)); }
  ;

STRING_LITERAL:
  '"' ( ESCAPE_SEQUENCE | ~('\\'|'"') )* '"' { setText(getText().substring(1, getText().length() - 1)); }
  |  ('\'' ( ESCAPE_SEQUENCE | ~('\\'|'\'') )* '\'')+ { setText(getText().substring(1, getText().length() - 1).replace("''", "'")); }
  ;

fragment ESCAPE_SEQUENCE:
  '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\')
  |  UNICODE_ESCAPE
  |  OCTAL_ESCAPE
  ;

fragment OCTAL_ESCAPE:
  '\\' ('0'..'3') ('0'..'7') ('0'..'7')
  |  '\\' ('0'..'7') ('0'..'7')
  |  '\\' ('0'..'7')
  ;

fragment UNICODE_ESCAPE:
  '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
  ;

REGEXP_LITERAL
   :  '/' ( ~ ( NL | '\\' | '/' ) | '\\' ~( NL ) )* '/' { setText(getText().substring(1, getText().length() - 1)); }
   ;

TO: ('t'|'T') ('o|O') ;

TRUE: ('t'|'T') ('r'|'R') ('u'|'U') ('e'|'E') ;

FALSE: ('f'|'F') ('a'|'A') ('l'|'L') ('s'|'S') ('e'|'E') ;

NULL: ('n'|'N') ('u'|'U') ('l'|'L') ('l'|'L') ;

EQUALS: '=' ;

COLON: ':' ;

NOT_EQUAL: '<>' | '!=' ;

PARAM: '?' ;

EXCLAMATION: '!' ;

GREATER: '>' ;

GREATER_EQUAL: '>=' ;

LESS: '<' ;

LESS_EQUAL: '<=' ;

AND: '&&' ;

OR: '||' ;

IDENTIFIER:
  ('a'..'z' | 'A'..'Z' | '_' | '$' | '\u0080'..'\ufffe') ('a'..'z' | 'A'..'Z' | '_' | '$' | '0'..'9' | '\u0080'..'\ufffe')*
  ;

QUOTED_IDENTIFIER:
  '`' ( ESCAPE_SEQUENCE | ~('\\' | '`') )* '`'
  ;

LPAREN: '(' ;

RPAREN: ')' ;

LSQUARE: '[' ;

RSQUARE: ']' ;

LCURLY: '{' ;

RCURLY: '}' ;

COMMA: ',' ;

DOT: '.' ;

PLUS: '+' ;

MINUS: '-' ;

ASTERISK: '*' ;

HASH: '#' ;

TILDE: '~' ;

CARAT: '^' ;
