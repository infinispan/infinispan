package org.infinispan.commons.configuration.attributes;

/**
 * AttributeValidator.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public interface AttributeValidator<T> {
   void validate(T value);
}
