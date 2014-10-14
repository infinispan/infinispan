package org.infinispan.cli.interpreter.statement;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.result.StringResult;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.util.logging.LogFactory;

/**
 *
 * CacheStatement shows the currently selected cache or selects a cache to be used as default for CLI operations
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class CacheStatement implements Statement {
   private static final Log log = LogFactory.getLog(CacheStatement.class, Log.class);

   final String cacheName;

   public CacheStatement(String cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   public Result execute(Session session) throws StatementException {
      if (cacheName != null) {
         session.setCurrentCache(cacheName);
         return EmptyResult.RESULT;
      } else {
         Cache<?, ?> currentCache = session.getCurrentCache();
         if (currentCache != null) {
            return new StringResult(currentCache.getName());
         } else {
            throw log.noCacheSelectedYet();
         }
      }
   }

}
