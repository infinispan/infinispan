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
