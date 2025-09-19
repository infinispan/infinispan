package org.infinispan.spring.common.config;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * @author Marius Bogoevici
 */
public class InfinispanRemoteCacheManagerBeanDefinitionParser extends AbstractBeanDefinitionParser {

   private static final String DEFAULT_CACHE_MANAGER_BEAN_NAME = "cacheManager";

   private static final String CACHE_MANAGER_CLASS = "org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean";

   @Override
   protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
      BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(CACHE_MANAGER_CLASS);

      String configFileLocation = element.getAttribute("configuration");
      if (StringUtils.hasText(configFileLocation)) {
         beanDefinitionBuilder.addPropertyValue("configurationPropertiesFileLocation", configFileLocation);
      }
      return beanDefinitionBuilder.getBeanDefinition();
   }

   @Override
   protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext) throws BeanDefinitionStoreException {
      String id = element.getAttribute("id");
      if (!StringUtils.hasText(id)) {
         id = DEFAULT_CACHE_MANAGER_BEAN_NAME;
      }
      return id;
   }
}
