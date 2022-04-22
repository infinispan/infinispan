package org.infinispan.api.common;

/**
 * @since 14.0
 **/
public interface Flag {
   Flags<?, ?> add(Flags<?, ?> flags);
}
