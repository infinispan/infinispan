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
package org.infinispan.query.objectfilter.impl.ql.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.infinispan.query.grammar.IckleLexer;
import org.infinispan.query.grammar.IckleParser;

abstract class TestBase {

   protected void expectParserSuccess(String inputText) {
      parse(inputText, false);
   }

   protected void expectParserSuccess(String inputText, String ignoredTreeOutput) {
      // Tree output format is different in ANTLR4, just verify parsing succeeds
      parse(inputText, false);
   }

   protected void expectParserFailure(String inputText) {
      parse(inputText, true);
   }

   protected void expectLexerSuccess(String inputText, String expectedTokenType) {
      parseLexer(inputText, false, expectedTokenType);
   }

   private void parseLexer(String inputText, boolean expectFailure, String expectedTokenType) {
      try {
         org.infinispan.query.grammar.IckleLexer lexer =
               new org.infinispan.query.grammar.IckleLexer(CharStreams.fromString(inputText));

         Token token = lexer.nextToken();

         if (token.getType() == Token.EOF) {
            if (expectFailure) return;
            fail("Lexer produced no token for: " + inputText);
         }

         if (expectedTokenType != null) {
            int expectedType = org.infinispan.query.grammar.IckleLexer.class
                  .getField(expectedTokenType).getInt(null);
            assertEquals("Token type for: " + inputText, expectedType, token.getType());
         }

         if (expectFailure) {
            fail("Lexing was expected to fail but succeeded: " + inputText);
         }
      } catch (AssertionError e) {
         throw e;
      } catch (Exception e) {
         if (expectFailure) return;
         fail(e.getMessage());
      }
   }

   private void parse(String inputText, boolean expectFailure) {
      try {
         IckleLexer lexer =
               new IckleLexer(CharStreams.fromString(inputText));

         CommonTokenStream tokens = new CommonTokenStream(lexer);
         IckleParser parser = new IckleParser(tokens);

         // Collect errors
         List<String> errors = new ArrayList<>();
         parser.removeErrorListeners();
         parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                                    int line, int charPositionInLine, String msg,
                                    RecognitionException e) {
               errors.add("line " + line + ":" + charPositionInLine + " " + msg);
            }
         });

         parser.statement();

         if (!errors.isEmpty()) {
            if (expectFailure) return;
            fail("Parse errors: " + errors);
         }

         if (expectFailure) {
            fail("Parsing was expected to fail but succeeded: " + inputText);
         }
      } catch (Exception e) {
         if (expectFailure) return;
         fail(e.getMessage());
      }
   }
}
