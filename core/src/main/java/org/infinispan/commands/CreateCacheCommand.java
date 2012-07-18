/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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

package org.infinispan.commands;

import java.util.concurrent.TimeUnit;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Command to create/start a cache on a subset of Infinispan cluster nodes
 * @author Vladimir Blagojevic
 * @since 5.2
 */
public class CreateCacheCommand extends BaseRpcCommand {

   private static final Log log = LogFactory.getLog(CreateCacheCommand.class);
   public static final byte COMMAND_ID = 29;

   private EmbeddedCacheManager cacheManager;
   private String cacheNameToCreate;
   private String cacheConfigurationName;

   private CreateCacheCommand() {
      super(null);
   }
   
   public CreateCacheCommand(String ownerCacheName) {
      super(ownerCacheName);      
   }

   public CreateCacheCommand(String ownerCacheName, String cacheNameToCreate, 
            String cacheConfigurationName) {
      super(ownerCacheName);      
      this.cacheNameToCreate = cacheNameToCreate;
      this.cacheConfigurationName = cacheConfigurationName;
   }
   
   public void init(EmbeddedCacheManager cacheManager){
      this.cacheManager = cacheManager;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      Configuration cacheConfig = cacheManager.getCacheConfiguration(cacheConfigurationName);
      if (cacheConfig == null) {
         // our sensible default
         cacheConfig = new ConfigurationBuilder().clustering().stateTransfer()
                  .fetchInMemoryState(false).unsafe().unreliableReturnValues(true).expiration()
                  .lifespan(2, TimeUnit.MINUTES).maxIdle(2, TimeUnit.MINUTES)
                  .wakeUpInterval(30, TimeUnit.SECONDS).enableReaper().clustering()
                  .cacheMode(CacheMode.DIST_SYNC).hash().numOwners(2).sync().transaction()
                  .transactionMode(TransactionMode.TRANSACTIONAL).syncCommitPhase(true)
                  .syncRollbackPhase(true).lockingMode(LockingMode.PESSIMISTIC).build();
         cacheManager.defineConfiguration(cacheNameToCreate, cacheConfig);
         log.debug("Using default tmp cache configuration, defined as " + cacheNameToCreate);
      }
      cacheManager.getCache(cacheNameToCreate); // getCache starts the cache as well*/
      log.debug("Defined and started cache " + cacheNameToCreate);
      return true;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[] {cacheNameToCreate, cacheConfigurationName};
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result
               + ((cacheConfigurationName == null) ? 0 : cacheConfigurationName.hashCode());
      result = prime * result + ((cacheNameToCreate == null) ? 0 : cacheNameToCreate.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj == null) {
         return false;
      }
      if (!(obj instanceof CreateCacheCommand)) {
         return false;
      }
      CreateCacheCommand other = (CreateCacheCommand) obj;
      if (cacheConfigurationName == null) {
         if (other.cacheConfigurationName != null) {
            return false;
         }
      } else if (!cacheConfigurationName.equals(other.cacheConfigurationName)) {
         return false;
      }
      if (cacheNameToCreate == null) {
         if (other.cacheNameToCreate != null) {
            return false;
         }
      } else if (!cacheNameToCreate.equals(other.cacheNameToCreate)) {
         return false;
      }
      return true;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Invalid method id " + commandId + " but " +
                  this.getClass() + " has id " + getCommandId());
      int i = 0;
      cacheNameToCreate = (String) parameters[i++];
      cacheConfigurationName = (String) parameters[i++];
   }

   @Override
   public String toString() {
      return "CreateCacheCommand [cacheNameToCreate=" + cacheNameToCreate
               + ", cacheConfigurationName=" + cacheConfigurationName + "]";
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }
}
