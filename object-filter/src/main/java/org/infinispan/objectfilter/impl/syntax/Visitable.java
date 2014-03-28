package org.infinispan.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface Visitable<T> {

   T acceptVisitor(Visitor visitor);
}
