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
package org.infinispan.objectfilter.impl.ql.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.Callable;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;
import org.infinispan.objectfilter.impl.ql.parse.IckleLexer;
import org.infinispan.objectfilter.impl.ql.parse.IckleParser;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
abstract class TestBase {

   protected void expectParserSuccess(String inputText) {
      parse(inputText, false, null, false);
   }

   protected void expectParserSuccess(String inputText, String expectedOut) {
      parse(inputText, false, expectedOut, false);
   }

   protected void expectParserFailure(String inputText) {
      parse(inputText, true, null, false);
   }

   protected void expectLexerSuccess(String inputText) {
      parse(inputText, false, null, true);
   }

   protected void expectLexerSuccess(String inputText, String expectedOut) {
      parse(inputText, false, expectedOut, true);
   }

   protected void expectLexerFailure(String inputText) {
      parse(inputText, true, null, true);
   }

   private void parse(String inputText, boolean expectFailure, String expectedTreeOut, boolean lexerOnly) {
      if (expectFailure && expectedTreeOut != null) {
         throw new IllegalArgumentException("If failure is expected then expectedTreeOut must be null");
      }

      IckleLexer lexer = new IckleLexer(new ANTLRStringStream(inputText));
      CommonTokenStream tokens = new CommonTokenStream(lexer);

      try {
         Object[] pair;
         if (lexerOnly) {
            pair = executeAndCaptureErrOut(() -> lexer.nextToken());
         } else {
            pair = executeAndCaptureErrOut(() -> {
               IckleParser parser = new IckleParser(tokens);
               IckleParser.statement_return statement = parser.statement();
               return statement.getTree();
            });
         }

         if (pair[1] != null) {
            // we have an error message
            if (expectFailure) {
               return;
            } else {
               fail((String) pair[1]);
            }
         }

         if (expectedTreeOut != null) {
            if (lexerOnly) {
               int expectedTokenType;
               try {
                  // expectedTreeOut is assumed to be a token name
                  Field tokenTypeConstant = lexer.getClass().getDeclaredField(expectedTreeOut);
                  expectedTokenType = (Integer) tokenTypeConstant.get(null);
               } catch (IllegalAccessException | NoSuchFieldException e) {
                  throw new RuntimeException("Could not determine the type of token: " + expectedTreeOut, e);
               }

               Token token = (Token) pair[0];
               assertEquals("Token type", expectedTokenType, token.getType());
            } else {
               CommonTree tree = (CommonTree) pair[0];
               assertEquals(expectedTreeOut, tree.toStringTree());
            }
         }

         String unconsumedTokens = getUnconsumedTokens(tokens);
         if (unconsumedTokens != null) {
            if (expectFailure) {
               return;
            } else {
               fail("Found unconsumed tokens: \"" + unconsumedTokens + "\".");
            }
         }
      } catch (Exception e) {
         if (expectFailure) {
            return;
         } else {
            fail(e.getMessage());
         }
      }

      if (expectFailure) {
         fail("Parsing was expected to fail but it actually succeeded.");
      }
   }

   private Object[] executeAndCaptureErrOut(Callable<?> callable) throws Exception {
      PrintStream oldErrStream = System.err;
      try {
         // no way to register an error handler so we capture any output written to System.err by the lexer/parser
         ByteArrayOutputStream errorStream = new ByteArrayOutputStream();
         System.setErr(new PrintStream(errorStream));
         Object retVal = callable.call();
         String errMsg = errorStream.size() > 0 ? errorStream.toString() : null;
         return new Object[]{retVal, errMsg};
      } finally {
         System.setErr(oldErrStream);
      }
   }

   private String getUnconsumedTokens(CommonTokenStream tokens) {
      // ensure we've buffered all tokens from the underlying TokenSource
      tokens.fill();
      if (tokens.index() == tokens.size() - 1) {
         return null;
      }

      StringBuilder sb = new StringBuilder();

      @SuppressWarnings("unchecked")
      List<Token> unconsumed = (List<Token>) tokens.getTokens(tokens.index(), tokens.size() - 1);
      for (Token t : unconsumed) {
         if (t.getType() != Token.EOF) {
            sb.append(t.getText());
         }
      }

      return sb.length() > 0 ? sb.toString() : null;
   }
}
