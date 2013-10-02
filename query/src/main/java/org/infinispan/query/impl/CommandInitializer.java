package org.infinispan.query.impl;

import org.hibernate.search.SearchFactory;
import org.hibernate.search.engine.spi.SearchFactoryImplementor;
import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.backend.QueryInterceptor;

/**
 * Initializes query module remote commands
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 * @since 5.1
 */
public final class CommandInitializer implements ModuleCommandInitializer {

   private Cache<?, ?> cache;
   private SearchFactoryImplementor searchFactoryImplementor;
   private QueryInterceptor queryInterceptor;
   private EmbeddedCacheManager cacheManager;
   
   public void setCache(Cache<?, ?> cache, EmbeddedCacheManager cacheManager){
	   this.cacheManager = cacheManager;
      this.cache = cache;
      SearchManager searchManager = Search.getSearchManager(cache);
      SearchFactory searchFactory = searchManager.getSearchFactory();
      searchFactoryImplementor = (SearchFactoryImplementor) searchFactory;
      queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
   }

   @Override
   public void initializeReplicableCommand(final ReplicableCommand c, final boolean isRemote) {
      //we don't waste cycles to check it's the correct type, as that would be a
      //critical error anyway: let it throw a ClassCastException.
      CustomQueryCommand queryCommand = (CustomQueryCommand) c;
      queryCommand.fetchExecutionContext(this);
   }

   public final SearchFactoryImplementor getSearchFactory() {
      return searchFactoryImplementor;
   }

   public final QueryInterceptor getQueryInterceptor() {
      return queryInterceptor;
   }
   
   public EmbeddedCacheManager getCacheManager(){
      return cacheManager;
   }

}
