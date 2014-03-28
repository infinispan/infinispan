package org.infinispan.objectfilter;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface Matcher {

   void match(Object instance);
}
