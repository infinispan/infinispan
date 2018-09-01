package org.infinispan.query.remote.impl;

/**
 * Identifiers used by the Marshaller to delegate to specialized Externalizers. For details, read
 * http://infinispan.org/docs/9.0.x/user_guide/user_guide.html#_preassigned_externalizer_id_ranges
 * <p/>
 * The range reserved for the Infinispan Remote Query module is from 1700 to 1799.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface ExternalizerIds {

   Integer PROTOBUF_VALUE_WRAPPER = 1700;

   Integer ICKLE_PROTOBUF_CACHE_EVENT_FILTER_CONVERTER = 1701;

   Integer ICKLE_PROTOBUF_FILTER_AND_CONVERTER = 1702;

   Integer ICKLE_CONTINUOUS_QUERY_CACHE_EVENT_FILTER_CONVERTER = 1703;

   Integer ICKLE_BINARY_PROTOBUF_FILTER_AND_CONVERTER = 1704;

   Integer ICKLE_CONTINUOUS_QUERY_RESULT = 1705;

   Integer ICKLE_FILTER_RESULT = 1706;

   Integer REMOTE_QUERY_DEFINITION = 1707;
}
