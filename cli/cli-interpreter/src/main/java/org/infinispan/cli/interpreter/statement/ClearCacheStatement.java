package org.infinispan.cli.interpreter.statement;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.session.Session;

/**
 *
 * ClearCacheStatement.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class ClearCacheStatement implements Statement {

   final String cacheName;

   public ClearCacheStatement(String cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   public Result execute(Session session) {
      Cache<?, ?> cache = session.getCache(cacheName);
      cache.clear();

      return EmptyResult.RESULT;
   }

}
