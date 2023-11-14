package org.infinispan.commons.configuration.attributes;

import org.infinispan.commons.util.Util;

/**
 * AttributeParser.
 *
 * @since 15.0
 */
public interface AttributeParser<T> {
   AttributeParser<Object> DEFAULT = Util::fromString;

   T parse(Class<?> klass, String value);
}
