package org.infinispan.commons.configuration.attributes;

/**
 * AttributeCopier. Usually an attribute is copied by using the {@link Object#clone()} method. When
 * this method is not enough, you can provide a custom attribute copier by implementing this
 * interface
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public interface AttributeCopier<T> {
   T copyAttribute(T attribute);
}
