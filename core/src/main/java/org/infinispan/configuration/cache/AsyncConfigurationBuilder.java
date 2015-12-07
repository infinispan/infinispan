package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.AsyncConfiguration.*;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.remoting.ReplicationQueueImpl;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * If configured all communications are asynchronous, in that whenever a thread sends a message sent
 * over the wire, it does not wait for an acknowledgment before returning. Asynchronous configuration is mutually
 * exclusive with synchronous configuration.
 */
public class AsyncConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder implements Builder<AsyncConfiguration> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   private final AttributeSet attributes;

   protected AsyncConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
      attributes = AsyncConfiguration.attributeDefinitionSet();
   }


   /**
    * @deprecated since 8.0
    */
   @Deprecated
   public AsyncConfigurationBuilder asyncMarshalling() {
      log.ignoreAsyncMarshalling();
      return this;
   }

   /**
    * @deprecated since 8.0
    */
   @Deprecated
   public AsyncConfigurationBuilder asyncMarshalling(boolean async) {
      log.ignoreAsyncMarshalling();
      return this;
   }

   /**
    * @deprecated since 8.0
    */
   @Deprecated
   public AsyncConfigurationBuilder syncMarshalling() {
      log.ignoreAsyncMarshalling();
      return this;
   }

   /**
    * The replication queue in use, by default {@link ReplicationQueueImpl}.
    *
    * @deprecated Since 8.2, superseded by JGroups bundler functionality.
    */
   @Deprecated
   public AsyncConfigurationBuilder replQueue(ReplicationQueue replicationQueue) {
      attributes.attribute(REPLICATION_QUEUE).set(replicationQueue);
      return this;
   }

   /**
    * If useReplQueue is set to true, this attribute controls how often the asynchronous thread used
    * to flush the replication queue runs.
    *
    * @deprecated Since 8.2, superseded by JGroups bundler functionality.
    */
   @Deprecated
   public AsyncConfigurationBuilder replQueueInterval(long interval) {
      attributes.attribute(REPLICATION_QUEUE_INTERVAL).set(interval);
      return this;
   }

   /**
    * If useReplQueue is set to true, this attribute controls how often the asynchronous thread used
    * to flush the replication queue runs.
    *
    * @deprecated Since 8.2, superseded by JGroups bundler functionality.
    */
   @Deprecated
   public AsyncConfigurationBuilder replQueueInterval(long interval, TimeUnit unit) {
      return replQueueInterval(unit.toMillis(interval));
   }

   /**
    * If useReplQueue is set to true, this attribute can be used to trigger flushing of the queue
    * when it reaches a specific threshold.
    *
    * @deprecated Since 8.2, superseded by JGroups bundler functionality.
    */
   @Deprecated
   public AsyncConfigurationBuilder replQueueMaxElements(int elements) {
      attributes.attribute(REPLICATION_QUEUE_MAX_ELEMENTS).set(elements);
      return this;
   }

   /**
    * If true, forces all async communications to be queued up and sent out periodically as a
    * batch.
    *
    * @deprecated Since 8.2, superseded by JGroups bundler functionality.
    */
   @Deprecated
   public AsyncConfigurationBuilder useReplQueue(boolean use) {
      attributes.attribute(USE_REPLICATION_QUEUE).set(use);
      return this;
   }

   @Override
   public
   void validate() {
      if (attributes.attribute(USE_REPLICATION_QUEUE).get() && getClusteringBuilder().cacheMode().isDistributed())
         throw log.noReplicationQueueDistributedCache();

      if (attributes.attribute(USE_REPLICATION_QUEUE).get() && getClusteringBuilder().cacheMode().isSynchronous())
         throw log.replicationQueueOnlyForAsyncCaches();
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public
   AsyncConfiguration create() {
      return new AsyncConfiguration(attributes.protect());
   }

   @Override
   public AsyncConfigurationBuilder read(AsyncConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return AsyncConfigurationBuilder.class.getSimpleName() + attributes;
   }
}
