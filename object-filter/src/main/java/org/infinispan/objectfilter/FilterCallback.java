package org.infinispan.objectfilter;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface FilterCallback {

   void onFilterResult(Object instance, Object[] projection, boolean isMatching);
}
