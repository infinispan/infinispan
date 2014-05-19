package org.infinispan.objectfilter;

/**
 * A single method callback that is used when registering a filter with the Matcher. The onFilterResult method is
 * notified of all successful matches. The instance being tested and the optional projection are passed back to the
 * subscriber. Implementations of this interface will be provided by the subscriber.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface FilterCallback {

   // TODO [anistor] the instance here could be a byte[] (or even a stream?) if the payload is Protobuf encoded
   /**
    * @param instance   the object being matched
    * @param projection the projection, if a projection was requested and this instance matches the filter, or null if
    *                   no projection was requested or this instance does not match
    */
   void onFilterResult(Object instance, Object[] projection);
}
