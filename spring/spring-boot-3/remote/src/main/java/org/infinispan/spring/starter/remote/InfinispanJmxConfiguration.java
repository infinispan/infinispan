package org.infinispan.spring.starter.remote;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.jmx.export.MBeanExporter;

/**
 * Spring and Infinispan register the same bean. Avoid an exception telling Spring not to export Infinispan's
 * bean
 *
 * @since 2.1.x
 */
@Configuration
@ConditionalOnProperty(prefix = "infinispan.remote", name = "jmx", havingValue = "true")
public class InfinispanJmxConfiguration {

   private final ObjectProvider<MBeanExporter> mBeanExporter;

   InfinispanJmxConfiguration(ObjectProvider<MBeanExporter> mBeanExporter) {
      this.mBeanExporter = mBeanExporter;
   }

   @PostConstruct
   public void excludeRemoteCacheManagerMBean() {
      this.mBeanExporter
            .ifUnique((exporter) -> exporter.addExcludedBean("remoteCacheManager"));
   }
}
