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
package org.infinispan.query.clustered.commandworkers;

import java.util.UUID;

import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.query.ISPNQuery;
import org.infinispan.query.clustered.QueryBox;

/**
 * ClusteredQueryCommandWorker.
 * 
 * Add specific behavior for ClusteredQueryCommand. Each ClusteredQueryCommandType links to a
 * ClusteredQueryCommandWorker
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public abstract class ClusteredQueryCommandWorker {

   protected Cache cache;

   private QueryBox queryBox;

   private SearchFactoryIntegrator searchFactory;

   // the query
   protected ISPNQuery query;
   protected UUID lazyQueryId;
   protected int docIndex;

   public void init(Cache cache, ISPNQuery query, UUID lazyQueryId, int docIndex) {
      this.cache = cache;
      this.query = query;
      this.lazyQueryId = lazyQueryId;
      this.docIndex = docIndex;
   }

   public abstract Object perform();

   protected QueryBox getQueryBox() {
      if (queryBox == null) {
         ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
         queryBox = cr.getLocalComponent(QueryBox.class);
      }

      return queryBox;
   }

   protected SearchFactoryIntegrator getSearchFactory() {
      if (searchFactory == null) {
         ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
         searchFactory = cr.getComponent(SearchFactoryIntegrator.class);
      }

      return searchFactory;
   }

}
