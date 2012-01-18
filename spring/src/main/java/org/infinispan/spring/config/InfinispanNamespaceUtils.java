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

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.TypedStringValue;
import org.springframework.beans.factory.parsing.BeanComponentDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.BeanDefinitionParserDelegate;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.core.Conventions;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

import java.util.List;

/**
 * @author Marius Bogoevici
 */
public class InfinispanNamespaceUtils {

    public static BeanComponentDefinition parseInnerBeanDefinition(Element element, ParserContext parserContext) {
		List<Element> childElements = DomUtils.getChildElementsByTagName(element, "bean");
		BeanComponentDefinition innerComponentDefinition = null;
		if (childElements != null && childElements.size() == 1) {
			Element beanElement = childElements.get(0);
            if (!"http://www.springframework.org/schema/beans".equals(beanElement.getNamespaceURI())) {
                throw new IllegalStateException ("Illegal inner child element");
            }
			BeanDefinitionParserDelegate delegate = parserContext.getDelegate();
			BeanDefinitionHolder beanDefinitionHolder = delegate.parseBeanDefinitionElement(beanElement);
			beanDefinitionHolder = delegate.decorateBeanDefinitionIfRequired(beanElement, beanDefinitionHolder);
			BeanDefinition beanDefinition = beanDefinitionHolder.getBeanDefinition();
			innerComponentDefinition = new BeanComponentDefinition(beanDefinition, beanDefinitionHolder.getBeanName());
		}
		return innerComponentDefinition;
	}

    public static void setPropertyIfAttributePresent(BeanDefinitionBuilder builder, Element element, String attributeName) {
		String attributeValue = element.getAttribute(attributeName);
		if (StringUtils.hasText(attributeValue)) {
			builder.addPropertyValue(Conventions.attributeNameToPropertyName(attributeName), new TypedStringValue(attributeValue));
		}
	}
}
