package org.infinispan.objectfilter.impl.predicateindex;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@FunctionalInterface
interface Condition<AttributeDomain> {

   boolean match(AttributeDomain attributeValue);
}
