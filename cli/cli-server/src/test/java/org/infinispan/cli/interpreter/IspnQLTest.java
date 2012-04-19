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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.infinispan.Cache;
import org.infinispan.cli.interpreter.IspnQLLexer;
import org.infinispan.cli.interpreter.IspnQLParser;
import org.infinispan.cli.interpreter.IspnQLParser.literal_return;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.cli.interpreter.statement.PutStatement;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class IspnQLTest {
   Session session;
   Cache cache;

   @BeforeMethod
   public void setup() {
      session = Mockito.mock(Session.class);
      cache = Mockito.mock(Cache.class);
      when(session.getCache()).thenReturn(cache);
   }

   public void testStringLiterals() throws IOException, RecognitionException {
      IspnQLParser parser = createParser("\"DoubleQuotedString\"");
      literal_return literal = parser.literal();
   }

   public void testPutStatement() throws IOException, RecognitionException {
      IspnQLParser parser = createParser("put 'a' 'b';");
      parser.statements();
      PutStatement s = (PutStatement) parser.statements.get(0);
      s.execute(session);
      verify(cache).put("a", "b");
   }

   public void testPutStatementExpiration() throws IOException, RecognitionException {
      IspnQLParser parser = createParser("put 'a' 'b' expires 1h;");
      parser.statements();
      PutStatement s = (PutStatement) parser.statements.get(0);
      s.execute(session);
      verify(cache).put("a", "b", 3600000, TimeUnit.MILLISECONDS);
   }

   public void testPutStatementExpirationIdle() throws IOException, RecognitionException {
      IspnQLParser parser = createParser("put 'a' 'b' expires 1h maxidle 30m;");
      parser.statements();
      PutStatement s = (PutStatement) parser.statements.get(0);
      s.execute(session);
      verify(cache).put("a", "b", 3600000, TimeUnit.MILLISECONDS, 1800000, TimeUnit.MILLISECONDS);
   }

   public void testPutCacheQualifier() throws IOException, RecognitionException {
      IspnQLParser parser = createParser("put 'myCache'.'a' 'b';");
      parser.statements();
      PutStatement s = (PutStatement) parser.statements.get(0);
      when(session.getCache("myCache")).thenReturn(cache);
      s.execute(session);
      verify(session).getCache("myCache");
      verify(cache).put("a", "b");
   }

   private IspnQLParser createParser(String testString) throws IOException {
      CharStream stream = new ANTLRStringStream(testString);
      IspnQLLexer lexer = new IspnQLLexer(stream);
      CommonTokenStream tokens = new CommonTokenStream(lexer);
      IspnQLParser parser = new IspnQLParser(tokens);
      return parser;
  }
}
