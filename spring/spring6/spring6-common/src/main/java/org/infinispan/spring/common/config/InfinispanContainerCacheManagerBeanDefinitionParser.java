package org.infinispan.spring.common.config;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.parsing.BeanComponentDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * @author Marius Bogoevici
 */
public class InfinispanContainerCacheManagerBeanDefinitionParser extends AbstractBeanDefinitionParser {

   private static final String DEFAULT_CACHE_MANAGER_BEAN_NAME = "cacheManager";

   private static final String FACTORY_BEAN_CLASS = "org.infinispan.spring.embedded.provider.ContainerEmbeddedCacheManagerFactoryBean";

   @Override
   protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
      BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(FACTORY_BEAN_CLASS);

      String cacheContainerRef = element.getAttribute("cache-container-ref");
      BeanComponentDefinition innerBean = InfinispanNamespaceUtils.parseInnerBeanDefinition(element, parserContext);
      if (innerBean != null) {
         parserContext.registerBeanComponent(innerBean);
      }
      if ((!StringUtils.hasText(cacheContainerRef) && innerBean == null)
            || (StringUtils.hasText(cacheContainerRef) && innerBean != null)) {
         parserContext.getReaderContext().error("Exactly one of the 'cache-container-ref' attribute " +
               "or an inner bean definition is required for a 'container-cache-manager' element", element);
      }
      beanDefinitionBuilder.addConstructorArgReference(innerBean != null ? innerBean.getBeanName() : cacheContainerRef);
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
