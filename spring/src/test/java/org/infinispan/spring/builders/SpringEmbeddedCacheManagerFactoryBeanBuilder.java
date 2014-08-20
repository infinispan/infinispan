package org.infinispan.spring.builders;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean;
import org.springframework.core.io.ClassPathResource;

/**
 * <p>
 * Builder for tests of SpringEmbeddedCacheManagerFactoryBean.
 * </p>
 *
 * @author <a href="mailto:sebastian.laskawiec@gmail.com">Sebastian Laskawiec</a>
 */
public class SpringEmbeddedCacheManagerFactoryBeanBuilder {

   private SpringEmbeddedCacheManagerFactoryBean buildingBean = new SpringEmbeddedCacheManagerFactoryBean();

   // in case this class will grow - use different types of factory methods instead of creating additional constructors
   private SpringEmbeddedCacheManagerFactoryBeanBuilder() {

   }

   /**
    * Creates default builder.
    *
    * @return This builder.
    */
   public static SpringEmbeddedCacheManagerFactoryBeanBuilder defaultBuilder() {
      return new SpringEmbeddedCacheManagerFactoryBeanBuilder();
   }

   /**
    * Reads configuration from file.
    *
    * @param configurationFile Configuration XML file.
    * @param aClass            Class for getting proper package from Class Loader.
    * @return This builder.
    */
   public SpringEmbeddedCacheManagerFactoryBeanBuilder fromFile(String configurationFile, Class<?> aClass) {
      buildingBean.setConfigurationFileLocation(new ClassPathResource(configurationFile, aClass));
      return this;
   }

   /**
    * Attaches global configuration to created SpringEmbeddedCacheManagerFactoryBean.
    *
    * @param globalConfigurationBuilder Global configuration instance.
    * @return This builder.
    */
   public SpringEmbeddedCacheManagerFactoryBeanBuilder withGlobalConfiguration(GlobalConfigurationBuilder globalConfigurationBuilder) {
      buildingBean.addCustomGlobalConfiguration(globalConfigurationBuilder);
      return this;
   }

   /**
    * Builds SpringEmbeddedCacheManagerFactoryBean.
    *
    * @return SpringEmbeddedCacheManagerFactoryBean instance.
    * @throws Exception Delegated from SpringEmbeddedCacheManagerFactoryBean#afterPropertiesSet.
    */
   public SpringEmbeddedCacheManagerFactoryBean build() throws Exception {
      buildingBean.afterPropertiesSet();
      return buildingBean;
   }

   /**
    * Attaches Configuration Builder to created SpringEmbeddedCacheManagerFactoryBean.
    *
    * @param configurationBuilder Configuration Builder instance.
    * @return This builder.
    */
   public SpringEmbeddedCacheManagerFactoryBeanBuilder withConfigurationBuilder(ConfigurationBuilder configurationBuilder) {
      buildingBean.addCustomCacheConfiguration(configurationBuilder);
      return this;
   }
}
