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
package org.infinispan.objectfilter.impl.ql.parse;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.antlr.runtime.EarlyExitException;
import org.antlr.runtime.FailedPredicateException;
import org.antlr.runtime.MismatchedNotSetException;
import org.antlr.runtime.MismatchedSetException;
import org.antlr.runtime.MismatchedTokenException;
import org.antlr.runtime.MismatchedTreeNodeException;
import org.antlr.runtime.NoViableAltException;
import org.antlr.runtime.Parser;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenStream;

/**
 * Base class for the generated parser. This class is stateful, so it should not be reused for parsing multiple
 * statements.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
abstract class ParserBase extends Parser {

   private final Deque<Boolean> enableParameterUsage = new ArrayDeque<>();

   private final List<String> errorMessages = new LinkedList<>();

   private int unaliasedCount = 0;

   protected ParserBase(TokenStream input, RecognizerSharedState state) {
      super(input, state);
   }

   protected final String buildUniqueImplicitAlias() {
      return "<gen:" + unaliasedCount++ + ">";
   }

   protected final boolean isParameterUsageEnabled() {
      return !enableParameterUsage.isEmpty() && enableParameterUsage.peek();
   }

   protected final void pushEnableParameterUsage(boolean enable) {
      enableParameterUsage.push(enable);
   }

   protected final void popEnableParameterUsage() {
      enableParameterUsage.pop();
   }

   protected final boolean validateSoftKeyword(String text) {
      return validateSoftKeyword(1, text);
   }

   protected final boolean validateSoftKeyword(int offset, String text) {
      if (input == null) {
         return false;
      }
      Token token = input.LT(offset);
      return token != null && text.equalsIgnoreCase(token.getText());
   }

   public final boolean hasErrors() {
      return !errorMessages.isEmpty();
   }

   public final List<String> getErrorMessages() {
      return errorMessages;
   }

   public final void reportError(RecognitionException e) {
      errorMessages.add(generateError(getRuleInvocationStack(e, getClass().getName()), getTokenNames(), e));
      super.reportError(e);
   }

   private String generateError(List invocationStack, String[] tokenNames, RecognitionException e) {
      String localization = invocationStack + ": line " + e.line + ":" + e.charPositionInLine + " ";
      return generateError(localization, tokenNames, e);
   }

   private String generateError(String localization, String[] tokenNames, RecognitionException e) {
      String message = "";
      if (e instanceof MismatchedTokenException) {
         MismatchedTokenException mte = (MismatchedTokenException) e;
         String tokenName = "<unknown>";
         if (mte.expecting == Token.EOF) {
            tokenName = "EOF";
         } else {
            if (tokenNames != null) {
               tokenName = tokenNames[mte.expecting];
            }
         }
         message = localization + "mismatched token: " + e.token + "; expecting type " + tokenName;
      } else if (e instanceof MismatchedTreeNodeException) {
         MismatchedTreeNodeException mtne = (MismatchedTreeNodeException) e;
         String tokenName = "<unknown>";
         if (mtne.expecting == Token.EOF) {
            tokenName = "EOF";
         } else {
            if (tokenNames != null) {
               tokenName = tokenNames[mtne.expecting];
            }
         }
         message = localization + "mismatched tree node: " + mtne.node + "; expecting type " + tokenName;
      } else if (e instanceof NoViableAltException) {
         NoViableAltException nvae = (NoViableAltException) e;
         message = localization + "state " + nvae.stateNumber + " (decision=" + nvae.decisionNumber + ") no viable alt; token=" + e.token;
      } else if (e instanceof EarlyExitException) {
         EarlyExitException eee = (EarlyExitException) e;
         message = localization + "required (...)+ loop (decision=" + eee.decisionNumber + ") did not match anything; token=" + e.token;
      } else if (e instanceof MismatchedNotSetException) {
         MismatchedNotSetException mse = (MismatchedNotSetException) e;
         message = localization + "mismatched token: " + e.token + "; expecting set " + mse.expecting;
      } else if (e instanceof MismatchedSetException) {
         MismatchedSetException mse = (MismatchedSetException) e;
         message = localization + "mismatched token: " + e.token + "; expecting set " + mse.expecting;
      } else if (e instanceof FailedPredicateException) {
         FailedPredicateException fpe = (FailedPredicateException) e;
         message = localization + "rule " + fpe.ruleName + " failed predicate: {" + fpe.predicateText + "}?";
      }
      return message;
   }
}
