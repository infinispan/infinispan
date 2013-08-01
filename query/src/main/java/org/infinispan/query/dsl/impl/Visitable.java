package org.infinispan.query.dsl.impl;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
interface Visitable {

   <ReturnType> ReturnType accept(Visitor<ReturnType> visitor);
}
