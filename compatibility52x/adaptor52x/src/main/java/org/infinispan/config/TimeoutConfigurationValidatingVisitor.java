package org.infinispan.config;

import org.infinispan.config.Configuration.CacheMode;
import org.infinispan.loaders.decorators.AsyncStoreConfig;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * TimeoutConfigurationValidatingVisitor checks transport related timeout relationships of
 * InfinispanConfiguration instance.
 * 
 * 
 * @author Vladimir Blagojevic
 * @since 5.0
 */
public class TimeoutConfigurationValidatingVisitor extends AbstractConfigurationBeanVisitor {

   private static final Log log = LogFactory.getLog(TimeoutConfigurationValidatingVisitor.class);

   private AsyncStoreConfig asyncType = null;
   private GlobalConfiguration global = null;

   @Override
   public void visitAsyncStoreConfig(AsyncStoreConfig bean) {
      asyncType = bean;
   }
   
   @Override
   public void visitGlobalConfiguration(GlobalConfiguration bean) {
      global = bean;
   }
   
   @Override
   public void visitConfiguration(Configuration bean) {
      
      boolean nonLocalCache = bean.getCacheMode() != CacheMode.LOCAL && global.getTransportClass() != null;
      if(nonLocalCache){
         if (asyncType != null && asyncType.getFlushLockTimeout() > asyncType.getShutdownTimeout())
            log.invalidTimeoutValue("<async>: flushLockTimeout ", asyncType.getFlushLockTimeout(),
                     "<async>: shutdownTimeout ", asyncType.getShutdownTimeout());
   
         if (asyncType != null && asyncType.getShutdownTimeout() > bean.getCacheStopTimeout())
            log.invalidTimeoutValue("<async>: shutdownTimeout ", asyncType.getShutdownTimeout(),
                     "<transaction>: cacheStopTimeout ", bean.getCacheStopTimeout());
   
         if (bean.getDeadlockDetectionSpinDuration() > bean.getLockAcquisitionTimeout())
            log.invalidTimeoutValue("<deadlockDetection>: spinDuration",
                     bean.getDeadlockDetectionSpinDuration(), "<locking>:lockAcquisitionTimeout ",
                     bean.getLockAcquisitionTimeout());
   
         if (asyncType != null && bean.getLockAcquisitionTimeout() > bean.getSyncReplTimeout())
            log.invalidTimeoutValue("<locking>:lockAcquisitionTimeout ",
                     bean.getLockAcquisitionTimeout(), "<sync>:replTimeout", bean.getSyncReplTimeout());
   
         if (asyncType != null && bean.getSyncReplTimeout() > global.getDistributedSyncTimeout())
            log.invalidTimeoutValue("<sync>:replTimeout", bean.getSyncReplTimeout(),
                     "<transport>: distributedSyncTimeout", global.getDistributedSyncTimeout());
   
         if (global.getDistributedSyncTimeout() > bean.getStateRetrievalTimeout())
            log.invalidTimeoutValue("<transport>: distributedSyncTimeout", global.getDistributedSyncTimeout(),
                     "<stateRetrieval>:timeout", bean.getStateRetrievalTimeout());
      }

   }
}
