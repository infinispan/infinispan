package org.infinispan.spring.embedded.builders;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.spring.embedded.provider.SpringEmbeddedCacheManagerFactoryBean;
import org.springframework.core.io.ClassPathResource;

/**
 * Builder for tests of SpringEmbeddedCacheManagerFactoryBean.
 *
 * @author Sebastian Laskawiec
 */
public class SpringEmbeddedCacheManagerFactoryBeanBuilder {

   private final SpringEmbeddedCacheManagerFactoryBean buildingBean = new SpringEmbeddedCacheManagerFactoryBean();

   private SpringEmbeddedCacheManagerFactoryBeanBuilder() {

   }

   public static SpringEmbeddedCacheManagerFactoryBeanBuilder defaultBuilder() {
      return new SpringEmbeddedCacheManagerFactoryBeanBuilder();
   }

   public SpringEmbeddedCacheManagerFactoryBeanBuilder fromFile(String configurationFile, Class<?> aClass) {
      buildingBean.setConfigurationFileLocation(new ClassPathResource(configurationFile, aClass));
      return this;
   }

   public SpringEmbeddedCacheManagerFactoryBeanBuilder withGlobalConfiguration(GlobalConfigurationBuilder globalConfigurationBuilder) {
      buildingBean.addCustomGlobalConfiguration(globalConfigurationBuilder);
      return this;
   }

   public SpringEmbeddedCacheManagerFactoryBean build() throws Exception {
      buildingBean.afterPropertiesSet();
      return buildingBean;
   }

   public SpringEmbeddedCacheManagerFactoryBeanBuilder withConfigurationBuilder(ConfigurationBuilder configurationBuilder) {
      buildingBean.addCustomCacheConfiguration(configurationBuilder);
      return this;
   }
}
