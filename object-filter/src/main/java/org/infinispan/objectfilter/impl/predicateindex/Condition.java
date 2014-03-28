package org.infinispan.objectfilter.impl.predicateindex;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
abstract class Condition<AttributeDomain> {

   public abstract boolean match(AttributeDomain attributeValue);
}
