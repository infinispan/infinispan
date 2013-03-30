/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.query;

import org.hibernate.search.SearchFactory;
import org.hibernate.search.engine.spi.SearchFactoryImplementor;
import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.impl.ComponentRegistryUtils;

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
