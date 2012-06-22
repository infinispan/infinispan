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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

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
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
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
   private static final long DEFAULT_SESSION_REAPER_WAKEUP_INTERVAL = 60000l; // in millis
   private static final long DEFAULT_SESSION_TIMEOUT = 60000l; // in millis

   private EmbeddedCacheManager cacheManager;
   private ScheduledExecutorService executor;
   private long sessionReaperWakeupInterval = DEFAULT_SESSION_REAPER_WAKEUP_INTERVAL;
   private long sessionTimeout = DEFAULT_SESSION_TIMEOUT;

   private final Map<String, Session> sessions = new ConcurrentHashMap<String, Session>();
   private ScheduledFuture<?> sessionReaperTask;

   public Interpreter() {
   }

   @Inject
   public void initialize(final EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
      this.executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
         @Override
         public Thread newThread(final Runnable r) {
            return new Thread(r, "Interpreter");
         }
      });
   }

   @Start
   public void start() {
      sessionReaperTask = executor.scheduleWithFixedDelay(new ScheduledTask(), sessionReaperWakeupInterval,
            sessionReaperWakeupInterval, TimeUnit.MILLISECONDS);
   }

   @Stop
   public void stop() {
      if (sessionReaperTask != null) {
         sessionReaperTask.cancel(true);
      }
   }

   @ManagedOperation(description = "Creates a new interpreter session")
   public String createSessionId() {
      String sessionId = UUID.randomUUID().toString();
      sessions.put(sessionId, new SessionImpl(cacheManager, sessionId));
      return sessionId;
   }

   public long getSessionReaperWakeupInterval() {
      return sessionReaperWakeupInterval;
   }

   public void setSessionReaperWakeupInterval(final long sessionReaperWakeupInterval) {
      this.sessionReaperWakeupInterval = sessionReaperWakeupInterval;
   }

   public long getSessionTimeout() {
      return sessionTimeout;
   }

   public void setSessionTimeout(final long sessionTimeout) {
      this.sessionTimeout = sessionTimeout;
   }

   void expireSessions() {
      long timeBoundary = System.nanoTime() - sessionTimeout * 1000000l;
      for (Iterator<Session> i = sessions.values().iterator(); i.hasNext();) {
         Session session = i.next();
         if (session.getTimestamp() < timeBoundary) {
            i.remove();
            if (log.isDebugEnabled()) {
               log.debugf("Removed expired interpreter session %s", session.getId());
            }
         }
      }
   }

   @ManagedOperation(description = "Parses and executes IspnQL statements")
   public String execute(final String sessionId, final String s) throws Exception {
      if (sessionId == null || !sessions.containsKey(sessionId)) {
         throw new IllegalArgumentException("Invalid session ID");
      }
      CharStream stream = new ANTLRStringStream(s);
      IspnQLLexer lexer = new IspnQLLexer(stream);
      CommonTokenStream tokens = new CommonTokenStream(lexer);
      IspnQLParser parser = new IspnQLParser(tokens);
      ClassLoader oldCL = SysPropertyActions.setThreadContextClassLoader(cacheManager.getCacheManagerConfiguration()
            .classLoader());
      Session session = null;
      try {
         parser.statements();
         session = sessions.get(sessionId);
         StringBuilder output = new StringBuilder();
         for (Statement stmt : parser.statements) {
            Result result = stmt.execute(session);
            if (result != EmptyResult.RESULT) {
               output.append(result.getResult());
            }
         }
         return output.length() == 0 ? null : output.toString();
      } catch (Exception e) {
         log.interpreterError(e);
         // Rewrap the exception into a plain exception to avoid the need for custom classes to travel via RMI
         Exception exception = new Exception(e.getMessage());
         exception.setStackTrace(e.getStackTrace());
         throw exception;
      } finally {
         if (session != null) {
            session.reset();
         }
         SysPropertyActions.setThreadContextClassLoader(oldCL);
      }
   }

   @ManagedAttribute(description = "Retrieves a list of caches for the cache manager")
   public String[] getCacheNames() {
      Set<String> cacheNames = new HashSet<String>(cacheManager.getCacheNames());
      cacheNames.add(BasicCacheContainer.DEFAULT_CACHE_NAME);
      return cacheNames.toArray(new String[0]);
   }

   class ScheduledTask implements Runnable {
      @Override
      public void run() {
         expireSessions();
      }
   }

}
