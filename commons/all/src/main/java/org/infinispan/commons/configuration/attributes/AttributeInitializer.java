package org.infinispan.commons.configuration.attributes;

/**
 * AttributeInitializer. Provides a way to initialize an attribute's value, whenever this needs to be done at Attribute construction time.
 * This is usually needed when the value is a mutable object.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public interface AttributeInitializer<T> {
   T initialize();
}
