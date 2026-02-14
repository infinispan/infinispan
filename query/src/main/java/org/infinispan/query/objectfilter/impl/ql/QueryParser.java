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
package org.infinispan.query.objectfilter.impl.ql;

import static java.util.stream.Collectors.toList;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.infinispan.query.grammar.IckleLexer;
import org.infinispan.query.grammar.IckleParser;
import org.infinispan.query.objectfilter.ParsingException;
import org.infinispan.query.objectfilter.impl.logging.Log;
import org.jboss.logging.Logger;

/**
 * A parser for Ickle queries. Parsing comprises these steps:
 * <ul>
 * <li>lexing the query</li>
 * <li>parsing the query, building up an AST while doing so</li>
 * <li>transforming the resulting parse tree using a QueryResolverDelegate and QueryRendererDelegate</li>
 * </ul>
 *
 * @author Gunnar Morling
 * @author anistor@redhat.com
 * @author Katia Aresti
 * @since 9.0
 */
public final class QueryParser {

   private static final Log log = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, QueryParser.class.getName());

   /**
    * Parses the given query string.
    *
    * @param ickle the ickle query string to parse
    * @return the result of the parsing after being transformed by the processors
    * @throws ParsingException in case any exception occurs during parsing
    */
   public IckleParser.StatementContext parseQuery(String ickle) throws ParsingException {
      CharStream input = CharStreams.fromString(ickle);
      IckleLexer lexer = new IckleLexer(input);
      CommonTokenStream tokens = new CommonTokenStream(lexer);
      IckleParser parser = new IckleParser(tokens);
      final ANTLRErrorListener errorListener = new BaseErrorListener() {
         @Override
         public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
            throw log.getQuerySyntaxException(ickle, prettifyAntlrError(offendingSymbol, line, charPositionInLine, msg, e, ickle, true));
         }
      };
      parser.addErrorListener(errorListener);
      return parser.statement();
   }

   /**
    * ANTLR's error messages are surprisingly bad,
    * so try to make them a bit better.
    * Thanks Hibernate
    */
   public static List<String> prettifyAntlrError(
         Object offendingSymbol,
         int line, int charPositionInLine,
         String message,
         RecognitionException e,
         String hql,
         boolean includeLocation) {
      String errorText = "";
      if (includeLocation) {
         errorText += "At " + line + ":" + charPositionInLine;
         if (offendingSymbol instanceof CommonToken commonToken) {
            String token = commonToken.getText();
            if (token != null && !token.isEmpty()) {
               errorText += " and token '" + token + "'";
            }
         }
         errorText += ", ";
      }
      if (e instanceof NoViableAltException) {
         errorText += message.substring(0, message.indexOf('\''));
         if (hql.isEmpty()) {
            errorText += "'*' (empty query string)";
         } else {
            String lineText = hql.lines().collect(toList()).get(line - 1);
            String text = lineText.substring(0, charPositionInLine) + "*" + lineText.substring(charPositionInLine);
            errorText += "'" + text + "'";
         }
      } else if (e instanceof InputMismatchException) {
         errorText += message.substring(0, message.length() - 1)
               .replace(" expecting {", ", expecting one of the following tokens: ");
      } else {
         errorText += message;
      }
      List<String> error = new ArrayList<>(1);
      error.add(errorText);
      return error;
   }
}
