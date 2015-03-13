package org.infinispan.commons.configuration.attributes;

public interface AttributeListener<T> {
   void attributeChanged(Attribute<T> newValue, Attribute<T> oldValue);
}
