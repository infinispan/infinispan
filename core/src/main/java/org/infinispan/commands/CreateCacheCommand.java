package org.infinispan.commands;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.manager.EmbeddedCacheManager;
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
   private boolean start;
   private int size;

   private CreateCacheCommand() {
      super(null);
   }

   public CreateCacheCommand(String ownerCacheName) {
      super(ownerCacheName);
   }

   public CreateCacheCommand(String ownerCacheName, String cacheNameToCreate, String cacheConfigurationName) {
      this(ownerCacheName, cacheNameToCreate, cacheConfigurationName, false, 0);
   }

   public CreateCacheCommand(String cacheName, String cacheNameToCreate, String cacheConfigurationName, boolean start, int size) {
      super(cacheName);
      this.cacheNameToCreate = cacheNameToCreate;
      this.cacheConfigurationName = cacheConfigurationName;
      this.start = start;
      this.size = size;
   }



   public void init(EmbeddedCacheManager cacheManager){
      this.cacheManager = cacheManager;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      Configuration cacheConfig = null;
      if (cacheConfigurationName != null) {
         cacheConfig = cacheManager.getCacheConfiguration(cacheConfigurationName);
         if (cacheConfig == null) {
            // Special case for the default temporary cache, which may or may not have been defined by the user
            if (MapReduceTask.DEFAULT_TMP_CACHE_CONFIGURATION_NAME.equals(cacheConfigurationName)) {
               cacheConfig = new ConfigurationBuilder().unsafe().unreliableReturnValues(true).clustering()
                     .cacheMode(CacheMode.DIST_SYNC).hash().numOwners(2).sync().build();
               log.debugf("Using default tmp cache configuration, defined as ", cacheNameToCreate);
            } else {
               throw new IllegalStateException("Cache configuration " + cacheConfigurationName
                     + " is not defined on node " + this.cacheManager.getAddress());
            }
         }
      }

      cacheManager.defineConfiguration(cacheNameToCreate, cacheConfig);
      cacheManager.getCache(cacheNameToCreate);
      log.debugf("Defined and started cache %s", cacheNameToCreate);
      return true;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[] {cacheNameToCreate, cacheConfigurationName, start, size};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Invalid method id " + commandId + " but " +
                                               this.getClass() + " has id " + getCommandId());
      int i = 0;
      cacheNameToCreate = (String) parameters[i++];
      cacheConfigurationName = (String) parameters[i++];
      start = (Boolean) parameters[i++];
      size = (Integer) parameters[i];
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
      return this.start == other.start && this.size == other.size;
   }

   @Override
   public String toString() {
      return "CreateCacheCommand{" +
            "cacheManager=" + cacheManager +
            ", cacheNameToCreate='" + cacheNameToCreate + '\'' +
            ", cacheConfigurationName='" + cacheConfigurationName + '\'' +
            ", start=" + start + '\'' +
            ", size=" + size +
            '}';
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
