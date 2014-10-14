package org.infinispan.cli.interpreter.statement;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.result.StringResult;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 *
 * InfoStatement shows configuration information about the specified cache or about the active cache manager
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class InfoStatement implements Statement {

   final String cacheName;

   public InfoStatement(String cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   public Result execute(final Session session) throws StatementException {
      if (cacheName != null) {
         return cacheInfo(session);
      } else {
         return cacheManagerInfo(session);
      }
   }

   private Result cacheManagerInfo(Session session) {
      EmbeddedCacheManager cacheManager = session.getCacheManager();
      GlobalConfiguration globalConfiguration = cacheManager.getCacheManagerConfiguration();
      return new StringResult(globalConfiguration.toString());
   }

   private Result cacheInfo(Session session) {
      Cache<?, ?> cache = session.getCache(cacheName);
      Configuration cacheConfiguration = cache.getCacheConfiguration();
      return new StringResult(cacheConfiguration.toString());
   }
}
