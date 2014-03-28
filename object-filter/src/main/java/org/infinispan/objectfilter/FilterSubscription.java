package org.infinispan.objectfilter;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface FilterSubscription {

   String getEntityTypeName();

   FilterCallback getCallback();

   String[] getProjection();
}
