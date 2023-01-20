package org.infinispan.commons.configuration.attributes;

import org.infinispan.commons.util.Util;

/**
 * AttributeParser.
 *
 * @since 15.0
 */
public interface AttributeParser<T> {
   AttributeParser<Object> DEFAULT = (klass, value) -> Util.fromString(klass, value);

   T parse(Class klass, String value);
}
