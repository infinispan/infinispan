package org.infinispan.cli.interpreter.statement;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.session.Session;

/**
 *
 * EvictStatement evicts an entry from the cache
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class EvictStatement implements Statement {
   final KeyData keyData;

   public EvictStatement(final KeyData key) {
      this.keyData = key;
   }

   @Override
   public Result execute(Session session) {
      Cache<Object, Object> cache = session.getCache(keyData.getCacheName());
      cache.getAdvancedCache().evict(keyData.getKey());
      return EmptyResult.RESULT;
   }

}
