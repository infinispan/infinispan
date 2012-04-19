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

import java.util.HashSet;
import java.util.Set;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.infinispan.api.BasicCacheContainer;
import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.cli.interpreter.session.SessionImpl;
import org.infinispan.cli.interpreter.statement.Statement;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.SysPropertyActions;
import org.infinispan.util.logging.LogFactory;

@Scope(Scopes.GLOBAL)
@MBean(objectName = "Interpreter", description = "Interpreter component which executes CLI operations")
public class Interpreter {
   private static final Log log = LogFactory.getLog(Interpreter.class, Log.class);

   private EmbeddedCacheManager cacheManager;

   public Interpreter() {
   }

   @Inject
   public void initialize(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @ManagedOperation(description = "Parses and executes IspnQL statements")
   public String execute(String s) throws Exception {
      CharStream stream = new ANTLRStringStream(s);
      IspnQLLexer lexer = new IspnQLLexer(stream);
      CommonTokenStream tokens = new CommonTokenStream(lexer);
      IspnQLParser parser = new IspnQLParser(tokens);
      ClassLoader oldCL = SysPropertyActions.setThreadContextClassLoader(cacheManager.getCacheManagerConfiguration()
            .classLoader());
      Session session = null;
      try {
         parser.statements();
         session = new SessionImpl(cacheManager);
         StringBuilder output = new StringBuilder();
         for (Statement stmt : parser.statements) {
            Result result = stmt.execute(session);
            if(result!=EmptyResult.RESULT) {
               output.append(result.getResult());
            }
         }
         return output.length()==0 ? null : output.toString();
     } catch (Exception e) {
         log.interpreterError(e);
         // Rewrap the exception into a plain exception to avoid the need for custom classes to travel via RMI
         Exception exception = new Exception(e.getMessage());
         exception.setStackTrace(e.getStackTrace());
         throw exception;
      } finally {
         if (session != null)
            session.reset();
         SysPropertyActions.setThreadContextClassLoader(oldCL);
      }
   }

   @ManagedAttribute(description="Retrieves a list of caches for the cache manager")
   public String[] getCacheNames() {
      Set<String> cacheNames = new HashSet<String>(cacheManager.getCacheNames());
      cacheNames.add(BasicCacheContainer.DEFAULT_CACHE_NAME);
      return cacheNames.toArray(new String[0]);
   }
}
