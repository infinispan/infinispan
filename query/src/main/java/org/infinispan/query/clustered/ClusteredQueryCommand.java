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
package org.infinispan.query.clustered;

import org.hibernate.search.query.engine.spi.HSQuery;
import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.query.CommandInitializer;
import org.infinispan.query.CustomQueryCommand;
import org.infinispan.query.ModuleCommandIds;
import org.infinispan.query.clustered.commandworkers.ClusteredQueryCommandWorker;

import java.util.UUID;

/**
 * Encapsulates all rpc calls for distributed queries actions
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class ClusteredQueryCommand extends BaseRpcCommand implements ReplicableCommand, CustomQueryCommand {

   public static final byte COMMAND_ID = ModuleCommandIds.CLUSTERED_QUERY;
   private static final Integer ZERO = Integer.valueOf(0);

   private ClusteredQueryCommandType commandType;

   private HSQuery query;

   // local instance (set only when command arrives on target node)
   private Cache<?, ?> cache;

   // identifies the query
   private UUID lazyQueryId;

   // for retrieve keys on a lazy query
   private Integer docIndex = ZERO;

   private ClusteredQueryCommand(ClusteredQueryCommandType type, String cacheName) {
      super(cacheName);
      commandType = type;
   }

   /**
    * For CommandFactory only. To create a ClusteredQueryCommand, use createLazyIterator(),
    * destroyLazyQuery(), getResultSize() or retrieveKeyFromLazyQuery()
    */
   public ClusteredQueryCommand(String cacheName) {
      super(cacheName);
   }

   @Override
   public void fetchExecutionContext(CommandInitializer ci) {
      this.cache = ci.getCacheManager().getCache(cacheName);
   }

   public static ClusteredQueryCommand createLazyIterator(HSQuery query, Cache<?, ?> cache, UUID id) {
      ClusteredQueryCommand clQuery = new ClusteredQueryCommand(ClusteredQueryCommandType.CREATE_LAZY_ITERATOR, cache.getName());
      clQuery.query = query;
      clQuery.lazyQueryId = id;
      return clQuery;
   }

   public static ClusteredQueryCommand getResultSize(HSQuery query, Cache<?, ?> cache) {
      ClusteredQueryCommand clQuery = new ClusteredQueryCommand(ClusteredQueryCommandType.GET_RESULT_SIZE, cache.getName());
      clQuery.query = query;
      return clQuery;
   }

   public static ClusteredQueryCommand createEagerIterator(HSQuery query, Cache<?, ?> cache) {
      ClusteredQueryCommand clQuery = new ClusteredQueryCommand(ClusteredQueryCommandType.CREATE_EAGER_ITERATOR, cache.getName());
      clQuery.query = query;
      return clQuery;
   }

   public static ClusteredQueryCommand destroyLazyQuery(Cache<?, ?> cache, UUID id) {
      ClusteredQueryCommand clQuery = new ClusteredQueryCommand(ClusteredQueryCommandType.DESTROY_LAZY_ITERATOR, cache.getName());
      clQuery.lazyQueryId = id;
      return clQuery;
   }

   public static ClusteredQueryCommand retrieveKeyFromLazyQuery(Cache<?, ?> cache, UUID id, int docIndex) {
      ClusteredQueryCommand clQuery = new ClusteredQueryCommand(ClusteredQueryCommandType.GET_SOME_KEYS, cache.getName());
      clQuery.lazyQueryId = id;
      clQuery.docIndex = docIndex;
      return clQuery;
   }

   public void initialize(Cache<?, ?> localInstance) {
      setCache(localInstance);
   }

   public void setCache(Cache<?, ?> cache) {
      this.cache = cache;
   }

   /**
    * Invokes a query on a (remote) cache and returns results (list of keys).
    * 
    * @param context
    *           invocation context, ignored.
    * @return returns an <code>List<Object></code>.
    */
   @Override
   public Object perform(InvocationContext context) throws Throwable {
      return perform(cache);
   }

   public QueryResponse perform(Cache<?, ?> cache) {
      ClusteredQueryCommandWorker worker = commandType.getCommand(cache, query, lazyQueryId, docIndex);
      return worker.perform();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[] { commandType, query, lazyQueryId, docIndex };
   }

   @Override
   public void setParameters(int commandId, Object[] args) {
      int i = 0;
      commandType = (ClusteredQueryCommandType) args[i++];
      query = (HSQuery) args[i++];
      lazyQueryId = (UUID) args[i++];
      docIndex = (Integer) args[i++];
   }

   @Override
   public String toString() {
      return "ClusteredQuery{ cache=" + getCacheName() + '}';
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((cacheName == null) ? 0 : cacheName.hashCode());
      result = prime * result + ((query == null) ? 0 : query.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (!(obj instanceof ClusteredQueryCommand))
         return false;
      ClusteredQueryCommand other = (ClusteredQueryCommand) obj;
      if (cacheName == null) {
         if (other.cacheName != null)
            return false;
      } else if (!cacheName.equals(other.cacheName))
         return false;
      if (query == null) {
         if (other.query != null)
            return false;
      } else if (!query.equals(other.query))
         return false;
      return true;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return true;
   }
}
