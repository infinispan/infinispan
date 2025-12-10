package org.infinispan.spring.common.config;

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

/**
 * {@link org.springframework.beans.factory.xml.NamespaceHandler} for Infinispan-based caches.
 *
 * @author Marius Bogoevici
 */
public class InfinispanNamespaceHandler extends NamespaceHandlerSupport {

   @Override
   public void init() {
      registerBeanDefinitionParser("embedded-cache-manager", new InfinispanEmbeddedCacheManagerBeanDefinitionParser());
      registerBeanDefinitionParser("remote-cache-manager", new InfinispanRemoteCacheManagerBeanDefinitionParser());
      registerBeanDefinitionParser("container-cache-manager", new InfinispanContainerCacheManagerBeanDefinitionParser());
   }

}
