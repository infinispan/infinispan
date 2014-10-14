package org.infinispan.cli.interpreter.statement;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.session.Session;

/**
 *
 * EndBatchStatement ends a running batch statement
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class EndBatchStatement implements Statement {
   final String cacheName;
   final boolean success;

   public EndBatchStatement(String cacheName, boolean success) {
      this.cacheName = cacheName;
      this.success = success;
   }

   @Override
   public Result execute(Session session) {
      Cache<Object, Object> cache = session.getCache(cacheName);
      cache.endBatch(success);
      return EmptyResult.RESULT;
   }

}
