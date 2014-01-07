package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.executors.ThreadPoolExecutorFactory;

import java.util.concurrent.ThreadFactory;

/**
 * @author Galder Zamarreño
 */
public class ThreadPoolConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<ThreadPoolConfiguration> {

   ThreadFactory threadFactory;
   ThreadPoolExecutorFactory threadPoolFactory;

   public ThreadPoolConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
   }

   @Override
   public void validate() {
      if (threadPoolFactory != null)
         threadPoolFactory.validate();
   }

   public ThreadPoolConfigurationBuilder threadFactory(ThreadFactory threadFactory) {
      this.threadFactory = threadFactory;
      return this;
   }

   public ThreadPoolConfigurationBuilder threadPoolFactory(ThreadPoolExecutorFactory threadPoolFactory) {
      this.threadPoolFactory = threadPoolFactory;
      return this;
   }

   @Override
   public ThreadPoolConfiguration create() {
      return new ThreadPoolConfiguration(threadFactory, threadPoolFactory);
   }

   @Override
   public ThreadPoolConfigurationBuilder read(ThreadPoolConfiguration template) {
      this.threadFactory = template.threadFactory();
      this.threadPoolFactory = template.threadPoolFactory();
      return this;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ThreadPoolConfigurationBuilder that = (ThreadPoolConfigurationBuilder) o;

      if (threadPoolFactory != null ? !threadPoolFactory.equals(that.threadPoolFactory) : that.threadPoolFactory != null)
         return false;
      if (threadFactory != null ? !threadFactory.equals(that.threadFactory) : that.threadFactory != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = threadFactory != null ? threadFactory.hashCode() : 0;
      result = 31 * result + (threadPoolFactory != null ? threadPoolFactory.hashCode() : 0);
      return result;
   }

}
