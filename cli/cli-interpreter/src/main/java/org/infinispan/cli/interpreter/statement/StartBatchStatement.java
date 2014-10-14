package org.infinispan.cli.interpreter.statement;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StringResult;
import org.infinispan.cli.interpreter.session.Session;

/**
 * Starts a new batch
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class StartBatchStatement implements Statement {
   final String cacheName;

   public StartBatchStatement(String cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   public Result execute(Session session) {
      Cache<Object, Object> cache = session.getCache(cacheName);
      boolean b = cache.startBatch();
      return b ? EmptyResult.RESULT : new StringResult("Batch for cache already started");
   }

}
