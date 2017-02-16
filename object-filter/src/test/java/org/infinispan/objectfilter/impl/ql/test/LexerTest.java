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

import org.junit.Test;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public class LexerTest extends TestBase {

   @Test
   public void testGreaterEqual() {
      // todo [anistor] it's also possible to invoke method mGREATER_EQUAL() of lexer directly instead of nextToken()
      expectLexerSuccess(">=", "GREATER_EQUAL");
   }

   @Test
   public void testIdentifier() {
      expectLexerSuccess("acme", "IDENTIFIER");
      expectLexerFailure("1cme");
   }

   @Test
   public void testFloatingPointLiteral() {
      expectLexerSuccess(".12e+10", "FLOATING_POINT_LITERAL");
      expectLexerFailure(".12e+-10");
   }

   @Test
   public void testNull() {
      expectLexerSuccess("null", "NULL");
      expectLexerSuccess("NULL", "NULL");
      expectLexerSuccess("NuLl", "NULL");
   }

   @Test
   public void testFalse() {
      expectLexerSuccess("false", "FALSE");
      expectLexerSuccess("FALSE", "FALSE");
      expectLexerSuccess("FaLse", "FALSE");
   }

   @Test
   public void testTrue() {
      expectLexerSuccess("true", "TRUE");
      expectLexerSuccess("TRUE", "TRUE");
      expectLexerSuccess("TrUe", "TRUE");
   }
}
