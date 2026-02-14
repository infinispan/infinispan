lexer grammar IckleLexer;

@header {
/*
 * SPDX-License-Identifier: Apache-2.0
 */
}

// ============================================================================
// WHITESPACE
// ============================================================================

WS: (' ' | '\t' | '\u000B' | '\f' | EOL)+ -> channel(HIDDEN);

fragment NL: '\r' | '\n';
fragment EOL: NL+;

// ============================================================================
// KEYWORDS - Case insensitive
// ============================================================================

// Note: ANTLR 4 requires keywords to be defined before IDENTIFIER
// to ensure they have priority

ALL:            A L L;
AND_KEYWORD:    A N D;
ANY:            A N Y;
AS:             A S;
ASC:            A S C;
AVG:            A V G;
BETWEEN:        B E T W E E N;
BOUNDINGBOX:    B O X;
CIRCLE:         C I R C L E;
COUNT:          C O U N T;
CROSS:          C R O S S;
DISTANCE:       D I S T A N C E;
DISTINCT:       D I S T I N C T;
DELETE:         D E L E T E;
DESC:           D E S C;
ELEMENTS:       E L E M E N T S;
EMPTY:          E M P T Y;
ESCAPE:         E S C A P E;
EXISTS:         E X I S T S;
FETCH:          F E T C H;
FILTERING:      F I L T E R I N G;
FROM:           F R O M;
FULL:           F U L L;
GROUP:          G R O U P;
HAVING:         H A V I N G;
IN:             I N;
INDEX:          I N D E X;
INDICES:        I N D I C E S;
INNER:          I N N E R;
IS:             I S;
JOIN:           J O I N;
KILOMETERS:     K I L O M E T E R S;
LEFT:           L E F T;
LIKE:           L I K E;
MAX:            M A X;
MEMBER:         M E M B E R;
METERS:         M E T E R S;
MILES:          M I L E S;
MIN:            M I N;
NAUTICAL_MILES: N A U T I C A L '_' M I L E S;
NOT:            N O T;
OBJECT:         O B J E C T;
OF:             O F;
ON:             O N;
OR_KEYWORD:     O R;
ORDER:          O R D E R;
OUTER:          O U T E R;
POLYGON:        P O L Y G O N;
PROPERTIES:     P R O P E R T I E S;
RIGHT:          R I G H T;
SCORE:          S C O R E;
SELECT:         S E L E C T;
SIZE:           S I Z E;
SOME:           S O M E;
SUM:            S U M;
VERSION:        V E R S I O N;
WHERE:          W H E R E;
WITH:           W I T H;
WITHIN:         W I T H I N;
YARDS:          Y A R D S;
BY:             B Y;

// Special keywords
TO:             T O;
TRUE:           T R U E;
FALSE:          F A L S E;
NULL:           N U L L;

// ============================================================================
// OPERATORS
// ============================================================================

EQUALS:         '=';
COLON:          ':';
NOT_EQUAL:      '<>' | '!=';
PARAM:          '?';
EXCLAMATION:    '!';
GREATER:        '>';
GREATER_EQUAL:  '>=';
LESS:           '<';
LESS_EQUAL:     '<=';
AND:            '&&';
OR:             '||';

// ============================================================================
// DELIMITERS
// ============================================================================

LPAREN:     '(';
RPAREN:     ')';
LSQUARE:    '[';
RSQUARE:    ']';
LCURLY:     '{';
RCURLY:     '}';
COMMA:      ',';
DOT:        '.';
PLUS:       '+';
MINUS:      '-';
ASTERISK:   '*';
HASH:       '#';
TILDE:      '~';
CARAT:      '^';
ARROW:      '<->';

// ============================================================================
// LITERALS
// ============================================================================

HEX_LITERAL:
    '0' [xX] HEX_DIGIT+ INTEGER_TYPE_SUFFIX?
    ;

INTEGER_LITERAL:
    '0'
    | [1-9] [0-9]*
    ;

DECIMAL_LITERAL:
    ('0' | [1-9] [0-9]*) INTEGER_TYPE_SUFFIX
    ;

OCTAL_LITERAL:
    '0' [0-7]+ INTEGER_TYPE_SUFFIX?
    ;

fragment HEX_DIGIT:
    [0-9a-fA-F]
    ;

fragment INTEGER_TYPE_SUFFIX:
    [lL]
    ;

FLOATING_POINT_LITERAL:
    [0-9]+ '.' [0-9]* EXPONENT? FLOAT_TYPE_SUFFIX?
    | '.' [0-9]+ EXPONENT? FLOAT_TYPE_SUFFIX?
    | [0-9]+ EXPONENT FLOAT_TYPE_SUFFIX?
    | [0-9]+ FLOAT_TYPE_SUFFIX
    ;

fragment EXPONENT:
    [eE] [+\-]? [0-9]+
    ;

fragment FLOAT_TYPE_SUFFIX:
    [fFdD]
    ;

CHARACTER_LITERAL:
    '\'' ( ESCAPE_SEQUENCE | ~['\\\r\n] ) '\''
    ;

STRING_LITERAL:
    '"' ( ESCAPE_SEQUENCE | ~[\\"\r\n] )* '"'
    | '\'' ( ESCAPE_SEQUENCE | ~[\\'\r\n] | '\'\'' )* '\''
    ;

fragment ESCAPE_SEQUENCE:
    '\\' [btnfr"'\\]
    | UNICODE_ESCAPE
    | OCTAL_ESCAPE
    ;

fragment OCTAL_ESCAPE:
    '\\' [0-3] [0-7] [0-7]
    | '\\' [0-7] [0-7]
    | '\\' [0-7]
    ;

fragment UNICODE_ESCAPE:
    '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;

REGEXP_LITERAL:
    '/' ( ~[\r\n\\/] | '\\' ~[\r\n] )* '/'
    ;

// ============================================================================
// IDENTIFIERS
// ============================================================================

IDENTIFIER:
    [a-zA-Z_$\u0080-\ufffe] [a-zA-Z_$0-9\u0080-\ufffe]*
    ;

QUOTED_IDENTIFIER:
    '`' ( ESCAPE_SEQUENCE | ~[\\`] )* '`'
    ;

// ============================================================================
// Case-insensitive fragments for keywords
// ============================================================================

fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];
