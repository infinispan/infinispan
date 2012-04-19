/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
grammar IspnQL;

options {
   language = Java;
   output = AST;
   rewrite = true;
}

@header {
/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.cli.interpreter;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectMapper.DefaultTyping;

import org.infinispan.cli.interpreter.session.*;
import org.infinispan.cli.interpreter.statement.*;
import static org.infinispan.cli.interpreter.utils.ParserSupport.*;

}

@members {
   private List<String> parserErrors = new ArrayList<String>();
   protected List<Statement> statements = new ArrayList<Statement>();
   private ObjectMapper jsonMapper = new ObjectMapper().enableDefaultTyping(DefaultTyping.NON_FINAL, JsonTypeInfo.As.WRAPPER_OBJECT);

   public void displayRecognitionError(String[] tokenNames, RecognitionException e) {
      parserErrors.add(getErrorHeader(e)+" "+getErrorMessage(e, tokenNames));
   }

   public List<String> getParserErrors() {
      return parserErrors;
   }
}

@lexer::header {
/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.cli.interpreter;
}

statements
   : (statement { statements.add($statement.stmt); } EOL*)* EOF
   ;

statement returns [Statement stmt]
   : abortBatchStatement { $stmt = $abortBatchStatement.stmt; }
   | beginTransactionStatement { $stmt = $beginTransactionStatement.stmt; }
   | cacheStatement { $stmt = $cacheStatement.stmt; }
   | clearStatement { $stmt = $clearStatement.stmt; }
   | commitTransactionStatement { $stmt = $commitTransactionStatement.stmt; }
   | endBatchStatement { $stmt = $endBatchStatement.stmt; }
   | evictStatement { $stmt = $evictStatement.stmt; }
   | getStatement { $stmt = $getStatement.stmt; }
   | locateStatement { $stmt = $locateStatement.stmt; }
   | putIfAbsentStatement { $stmt = $putIfAbsentStatement.stmt; }
   | putStatement { $stmt = $putStatement.stmt; }
   | removeStatement { $stmt = $removeStatement.stmt; }
   | replaceStatement { $stmt = $replaceStatement.stmt; }
   | rollbackTransactionStatement { $stmt = $rollbackTransactionStatement.stmt; }
   | startBatchStatement { $stmt = $startBatchStatement.stmt; }
   ;


abortBatchStatement returns [EndBatchStatement stmt]
   : ABORT (cacheName = STRINGLITERAL)? (EOL | ';')! { $stmt = new EndBatchStatement(unquote($cacheName.text), false); }
   ;

beginTransactionStatement returns [BeginTransactionStatement stmt]
   : BEGIN (cacheName = STRINGLITERAL)? (EOL | ';')! { $stmt = new BeginTransactionStatement(unquote($cacheName.text)); }
   ;

cacheStatement returns [CacheStatement stmt]
   : CACHE (cacheName = STRINGLITERAL)? (EOL | ';')! { $stmt = new CacheStatement(unquote($cacheName.text)); }
   ;

clearStatement returns [ClearStatement stmt]
   : CLEAR (EOL | ';')! { $stmt = new ClearStatement(); }
   ;

commitTransactionStatement returns [CommitTransactionStatement stmt]
   : COMMIT (cacheName = STRINGLITERAL)? (EOL | ';')! { $stmt = new CommitTransactionStatement(unquote($cacheName.text)); }
   ;

endBatchStatement returns [EndBatchStatement stmt]
   : END (cacheName = STRINGLITERAL)? (EOL | ';')! { $stmt = new EndBatchStatement(unquote($cacheName.text), true); }
   ;

evictStatement returns [EvictStatement stmt]
   : EVICT key = keyIdentifier (EOL | ';')! { $stmt = new EvictStatement($key.key); }
   ;

getStatement returns [GetStatement stmt]
   : GET key = keyIdentifier (EOL | ';')! { $stmt = new GetStatement($key.key); }
   ;

locateStatement returns [LocateStatement stmt]
   : LOCATE key = keyIdentifier (EOL | ';')! { $stmt = new LocateStatement($key.key); }
   ;

putIfAbsentStatement returns [PutIfAbsentStatement stmt]
   : PUTIFABSENT key = keyIdentifier value = literal (exp = expirationClause)? (EOL | ';')! { $stmt = new PutIfAbsentStatement($key.key, $value.o, $exp.exp); }
   ;

putStatement returns [PutStatement stmt]
   : PUT key = keyIdentifier value = literal (exp = expirationClause)? (EOL | ';')! { $stmt = new PutStatement($key.key, $value.o, $exp.exp); }
   ;

removeStatement returns [RemoveStatement stmt]
   : REMOVE key = keyIdentifier (value = literal)? (EOL | ';')! { $stmt = new RemoveStatement($key.key, $value.o); }
   ;

replaceStatement returns [ReplaceStatement stmt]
   : REPLACE key = keyIdentifier value1 = literal (value2 = literal)? (exp = expirationClause)? (EOL | ';')! { if ($value2.o==null) $stmt = new ReplaceStatement($key.key, $value1.o, $exp.exp); else $stmt = new ReplaceStatement($key.key, $value1.o, $value2.o, $exp.exp);}
   ;

rollbackTransactionStatement returns [RollbackTransactionStatement stmt]
   : ROLLBACK (cacheName = STRINGLITERAL)? (EOL | ';')! { $stmt = new RollbackTransactionStatement(unquote($cacheName.text)); }
   ;

startBatchStatement returns [StartBatchStatement stmt]
   : START (cacheName = STRINGLITERAL)? (EOL | ';')! { $stmt = new StartBatchStatement(unquote($cacheName.text)); }
   ;

expirationClause returns [ExpirationData exp]
   : EXPIRES expires = timeLiteral (MAXIDLE idle = timeLiteral)? { $exp = new ExpirationData($expires.l, $idle.l); }
   ;

keyIdentifier returns [KeyData key]
   : STRINGLITERAL '.' literal { $key = new KeyData(unquote($STRINGLITERAL.text), $literal.o); }
   | literal { $key = new KeyData($literal.o); }
   ;

literal returns [Object o]
   : INTLITERAL { $o = Integer.valueOf($INTLITERAL.text); }
   | LONGLITERAL { $o = Long.valueOf($LONGLITERAL.text); }
   | FLOATLITERAL { $o = Float.valueOf($FLOATLITERAL.text); }
   | DOUBLELITERAL { $o = Double.valueOf($DOUBLELITERAL.text); }
   | NULL { $o = null; }
   | STRINGLITERAL { $o = unquote($STRINGLITERAL.text); }
   | UUIDLITERAL { $o = UUID.fromString($UUIDLITERAL.text); }
   | b = ( TRUE | FALSE ) { $o = Boolean.valueOf($b.text); }
   | jsonLiteral {
         $o = jsonMapper.readValue($jsonLiteral.text, Object.class);
   }
   ;
   catch[JsonProcessingException jpe] { throw new RecognitionException(input); }
   catch[IOException ioe] {}

timeLiteral returns [Long l]
   : INTLITERAL TIMESUFFIX { $l = millis($INTLITERAL.text, $TIMESUFFIX.text); }
   ;

jsonLiteral
   : '{' jsonPair (',' jsonPair)* '}'
   ;

jsonPair
   : jsonString ':' jsonValue
   ;

jsonString
   : STRINGLITERAL
   ;

jsonValue
   : jsonLiteral
   | jsonArray
   | INTLITERAL
   | DOUBLELITERAL
   | TRUE | FALSE
   | NULL
   | jsonString
   ;

jsonArray
   : '[' jsonValue (',' jsonValue)* ']'
   ;

/***************************************************************
 * LEXER RULES
 ***************************************************************/

INTLITERAL
   : IntegerNumber
   ;

LONGLITERAL
    : IntegerNumber LongSuffix
    ;

STRINGLITERAL
   : '"' DoubleQuotedStringCharacter* '"'
   | '\'' SingleQuotedStringCharacter* '\''
   ;

FLOATLITERAL
   : NonIntegerNumber FloatSuffix
   ;

DOUBLELITERAL
   : NonIntegerNumber DoubleSuffix?
   ;

UUIDLITERAL
   : HexDigit HexDigit HexDigit HexDigit HexDigit HexDigit HexDigit HexDigit '-'
     HexDigit HexDigit HexDigit HexDigit '-'
     HexDigit HexDigit HexDigit HexDigit '-'
     HexDigit HexDigit HexDigit HexDigit '-'
     HexDigit HexDigit HexDigit HexDigit HexDigit HexDigit HexDigit HexDigit HexDigit HexDigit HexDigit HexDigit
   ;
   

TIMESUFFIX
   : 'd' | 'h' | 'm' | 's' | 'ms'
   ;

fragment
AlphaChar
   : ('A'..'Z'|'a'..'z')
   ;

fragment
AlphaNumChar
   : AlphaChar
   | DecimalDigit
   ;

fragment
IntegerNumber
   : '0'
   | '1'..'9' DecimalDigit*
   | '0' ('0'..'7')+
   | HexPrefix HexDigit+
   ;

fragment
DecimalDigit
   : ('0'..'9')
   ;

fragment
HexPrefix
   : '0x' | '0X'
   ;
      
fragment
HexDigit
   : ('0'..'9'|'a'..'f'|'A'..'F')
   ;

fragment
LongSuffix
   : 'l' | 'L'
   ;

fragment
NonIntegerNumber
   : DecimalDigit+ '.' DecimalDigit* Exponent?  
   | '.' DecimalDigit+ Exponent?  
   | DecimalDigit+ Exponent
   ;

fragment 
Exponent   
   : ( 'e' | 'E' ) ( '+' | '-' )? ( '0' .. '9' )+ 
   ;

fragment 
FloatSuffix
   : 'f' | 'F' 
   ;

fragment
DoubleSuffix
   : 'd' | 'D'
   ;

fragment
DoubleQuotedStringCharacter
   : ~('"' | '\\' | EOL)
   | '\\' EscapeSequence
   ;

fragment
SingleQuotedStringCharacter
   : ~('\'' | '\\' | EOL)
   | '\\' EscapeSequence
   ;

fragment EscapeSequence
   : CharacterEscapeSequence
   | '0'
   | HexEscapeSequence
   | UnicodeEscapeSequence
   ;
   
fragment CharacterEscapeSequence
   : SingleEscapeCharacter
   | NonEscapeCharacter
   ;

fragment NonEscapeCharacter
   : ~(EscapeCharacter | EOL)
   ;

fragment SingleEscapeCharacter
   : '\'' | '"' | '\\' | 'b' | 'f' | 'n' | 'r' | 't' | 'v'
   ;

fragment EscapeCharacter
   : SingleEscapeCharacter
   | DecimalDigit
   | 'x'
   | 'u'
   ;

fragment HexEscapeSequence
   : 'x' HexDigit HexDigit
   ;

fragment UnicodeEscapeSequence
   : 'u' HexDigit HexDigit HexDigit HexDigit
   ;

Comment
   : '/*' (options {greedy=false;} : .)* '*/' { $channel=HIDDEN; }
   ;

LineComment
   : '//' ~(EOL)* { skip(); }
   ;

EOL
   : '\n'   // Line feed.
   | '\r'   // Carriage return.
   | '\u2028'  // Line separator.
   | '\u2029'  // Paragraph separator.
   ;

ABORT:   'abort';
BEGIN:   'begin';
CACHE:   'cache';
CLEAR:   'clear';
COMMIT:  'commit';
END:     'end';
EVICT:   'evict';
EXPIRES: 'expires';
FALSE:   'false';
GET:     'get';
LOCATE:  'locate';
MAXIDLE: 'maxidle';
NULL:    'null';
PUT:     'put';
PUTIFABSENT:     'putifabsent';
REMOVE:  'remove';
REPLACE: 'replace';
ROLLBACK:'rollback';
START:   'start';
TRUE:    'true';

WhiteSpace // Tab, vertical tab, form feed, space, non-breaking space and any other unicode "space separator".
   : ('\t' | '\v' | '\f' | ' ' | '\u00A0') { $channel=HIDDEN; }
   ;
