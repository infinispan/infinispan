package org.infinispan.commons.configuration.attributes;

/**
 * An AttributeListener will be invoked whenever an attribute has been modified.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public interface AttributeListener<T> {
   void attributeChanged(Attribute<T> attribute, T oldValue);
}
