package org.infinispan.configuration.cache;

import org.infinispan.config.ConfigurationException;

/**
 * Thread pool configuration for total order protocol
 *
 * This is only active when we use Total Order with Repeatable Read, Write Skew Check and Versions
 *
 * Date: 1/17/12
 * Time: 11:44 AM
 *
 * @author pruivo
 */
public class TotalOrderThreadingConfigurationBuilder extends AbstractTransportConfigurationChildBuilder<TotalOrderThreadingConfiguration> {

   private int corePoolSize = 1;

   private int maximumPoolSize = 8;

   private long keepAliveTime = 1000; //milliseconds

   private int queueSize = 100;

   //With write skew check, the commit can be done in one phase or two without loosing consistency
   private boolean onePhaseCommit = false;

   protected TotalOrderThreadingConfigurationBuilder(TransactionConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   void validate() {
      if(corePoolSize <= 0 || keepAliveTime <= 0 || maximumPoolSize <= 0) {
         throw new ConfigurationException("All the configuration values (corePoolSize, keepAliveTime, " +
               "maximumPoolSize) must be greater than zero");
      } else if(corePoolSize > maximumPoolSize) {
         throw new ConfigurationException("Core pool size value is greater than the maximum pool size");
      } else if(queueSize <= 0) {
         throw new ConfigurationException("Queue size must be greater than zero");
      }

   }

   @Override
   TotalOrderThreadingConfiguration create() {
      return new TotalOrderThreadingConfiguration(corePoolSize, maximumPoolSize, keepAliveTime, queueSize,
            onePhaseCommit);
   }

   @Override
   public ConfigurationChildBuilder read(TotalOrderThreadingConfiguration template) {
      this.corePoolSize = template.getCorePoolSize();
      this.maximumPoolSize = template.getMaximumPoolSize();
      this.keepAliveTime = template.getKeepAliveTime();
      this.queueSize = template.getQueueSize();
      this.onePhaseCommit = template.isOnePhaseCommit();
      return this;
   }

   public TotalOrderThreadingConfigurationBuilder corePoolSize(int corePoolSize) {
      this.corePoolSize = corePoolSize;
      return this;
   }

   public TotalOrderThreadingConfigurationBuilder maximumPoolSize(int maximumPoolSize) {
      this.maximumPoolSize = maximumPoolSize;
      return this;
   }

   public TotalOrderThreadingConfigurationBuilder keepAliveTime(long keepAliveTime) {
      this.keepAliveTime = keepAliveTime;
      return this;
   }

   public TotalOrderThreadingConfigurationBuilder queueSize(int queueSize) {
      this.queueSize = queueSize;
      return this;
   }

   public TotalOrderThreadingConfigurationBuilder onePhaseCommit(boolean onePhaseCommit) {
      this.onePhaseCommit = onePhaseCommit;
      return this;
   }
}
