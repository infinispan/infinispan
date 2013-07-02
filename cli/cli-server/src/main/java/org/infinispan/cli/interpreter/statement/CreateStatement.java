package org.infinispan.cli.interpreter.statement;

import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.session.Session;

/**
 * CreateStatement creates a new cache based on the configuration of an existing cache.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class CreateStatement implements Statement {

   final String cacheName;
   final String baseCacheName;

   public CreateStatement(String cacheName, String baseCacheName) {
      this.cacheName = cacheName;
      this.baseCacheName = baseCacheName;
   }

   @Override
   public Result execute(Session session) {
      session.createCache(cacheName, baseCacheName);
      return EmptyResult.RESULT;
   }

}
