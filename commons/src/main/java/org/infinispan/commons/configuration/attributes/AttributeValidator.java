package org.infinispan.commons.configuration.attributes;

public interface AttributeValidator<T> {
   void validate(T value);
}
