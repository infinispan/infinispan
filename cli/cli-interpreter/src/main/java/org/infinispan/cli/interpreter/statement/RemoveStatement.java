package org.infinispan.cli.interpreter.statement;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.session.Session;

/**
 * Implements the "rm [cache.]key" statetement which removes the specified key from a cache
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class RemoveStatement implements Statement {
   final KeyData keyData;
   final Object value;

   public RemoveStatement(final KeyData key, final Object value) {
      this.keyData = key;
      this.value = value;
   }

   @Override
   public Result execute(Session session) {
      Cache<Object, Object> cache = session.getCache(keyData.getCacheName());
      if (value == null) {
         cache.remove(keyData.getKey());
      } else {
         cache.remove(keyData.getKey(), value);
      }
      return EmptyResult.RESULT;
   }

}
