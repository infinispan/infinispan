package org.infinispan.api.common;

/**
 * @since 14.0
 **/
public interface Flags<F extends Flag, SELF> {
   SELF add(F flag);

   boolean contains(F flag);

   SELF addAll(Flags<F, SELF> flags);
}
