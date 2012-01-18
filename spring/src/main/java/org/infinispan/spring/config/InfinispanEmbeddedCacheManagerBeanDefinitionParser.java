/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *    ~
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.spring.config;

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
public class InfinispanEmbeddedCacheManagerBeanDefinitionParser extends AbstractBeanDefinitionParser {

    private static final String DEFAULT_CACHE_MANAGER_BEAN_NAME = "cacheManager";

    private static final String CACHE_MANAGER_CLASS = "org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean";

    @Override
    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
      BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(CACHE_MANAGER_CLASS);
      String configFileLocation = element.getAttribute("configuration");
      if (StringUtils.hasText(configFileLocation)) {
         beanDefinitionBuilder.addPropertyValue("configurationFileLocation", configFileLocation);
      }
      return beanDefinitionBuilder.getBeanDefinition();
    }

    @Override
   protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext) throws BeanDefinitionStoreException
   {
      String id = element.getAttribute("id");
      if (!StringUtils.hasText(id))
      {
         id = DEFAULT_CACHE_MANAGER_BEAN_NAME;
      }
      return id;
   }
}
