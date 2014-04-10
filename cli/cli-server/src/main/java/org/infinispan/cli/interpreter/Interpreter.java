package org.infinispan.cli.interpreter;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
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
import org.infinispan.cli.interpreter.codec.CodecRegistry;
import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.ResultKeys;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.cli.interpreter.session.SessionImpl;
import org.infinispan.cli.interpreter.statement.Statement;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.TimeService;
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
   private CodecRegistry codecRegistry;
   private TimeService timeService;

   private final Map<String, Session> sessions = new ConcurrentHashMap<String, Session>();
   private ScheduledFuture<?> sessionReaperTask;

   public Interpreter() {
   }

   @Inject
   public void initialize(final EmbeddedCacheManager cacheManager, TimeService timeService) {
      this.cacheManager = cacheManager;
      this.codecRegistry = new CodecRegistry(cacheManager.getCacheManagerConfiguration().classLoader());
      this.timeService = timeService;
   }

   @Start
   public void start() {
      this.executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
         @Override
         public Thread newThread(final Runnable r) {
            return new Thread(r, "Interpreter");
         }
      });
      sessionReaperTask = executor.scheduleWithFixedDelay(new ScheduledTask(), sessionReaperWakeupInterval, sessionReaperWakeupInterval, TimeUnit.MILLISECONDS);
   }

   @Stop
   public void stop() {
      if (sessionReaperTask != null) {
         sessionReaperTask.cancel(true);
      }
      executor.shutdownNow();
   }

   @ManagedOperation(description = "Creates a new interpreter session")
   public String createSessionId(String cacheName) {
      String sessionId = UUID.randomUUID().toString();
      SessionImpl session = new SessionImpl(codecRegistry, cacheManager, sessionId, timeService);
      sessions.put(sessionId, session);
      if (cacheName != null) {
         session.setCurrentCache(cacheName);
      }
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
      long timeBoundary = timeService.time() - sessionTimeout * 1000000l;
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

   @ManagedOperation(description = "Parses and executes IspnCliQL statements")
   public Map<String, String> execute(final String sessionId, final String s) throws Exception {
      Session session = null;
      ClassLoader oldCL = SecurityActions.setThreadContextClassLoader(cacheManager.getCacheManagerConfiguration().classLoader());
      Map<String, String> response = new HashMap<String, String>();
      try {
         session = validateSession(sessionId);

         CharStream stream = new ANTLRStringStream(s);
         IspnCliQLLexer lexer = new IspnCliQLLexer(stream);
         CommonTokenStream tokens = new CommonTokenStream(lexer);
         IspnCliQLParser parser = new IspnCliQLParser(tokens);

         parser.statements();

         if (parser.hasParserErrors()) {
            throw new ParseException(parser.getParserErrors());
         }

         StringBuilder output = new StringBuilder();
         for (Statement stmt : parser.statements) {
            Result result = stmt.execute(session);
            if (result != EmptyResult.RESULT) {
               output.append(result.getResult());
            }
         }
         if (output.length() > 0) {
            response.put(ResultKeys.OUTPUT.toString(), output.toString());
         }
      } catch (Throwable t) {
         log.interpreterError(t);
         response.put(ResultKeys.ERROR.toString(), t.getMessage());
         StringWriter sw = new StringWriter();
         PrintWriter pw = new PrintWriter(sw);
         t.printStackTrace(pw);
         response.put(ResultKeys.STACKTRACE.toString(), sw.toString());
      } finally {
         if (session != null) {
            session.reset();
            response.put(ResultKeys.CACHE.toString(), session.getCurrentCacheName());
         }
         SecurityActions.setThreadContextClassLoader(oldCL);

      }
      return response;
   }

   private Session validateSession(final String sessionId) {
      if (sessionId == null) {
         Session session = new SessionImpl(codecRegistry, cacheManager, null, timeService);
         session.setCurrentCache(BasicCacheContainer.DEFAULT_CACHE_NAME);
         return session;
      }
      if (!sessions.containsKey(sessionId)) {
         throw log.invalidSession(sessionId);
      }
      return sessions.get(sessionId);
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
