package org.infinispan.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface ValueExpr extends Visitable<ValueExpr> {

   String toJpaString();
}
