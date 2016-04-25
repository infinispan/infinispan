package org.infinispan.query.remote.impl;

/**
 * Identifiers used by the Marshaller to delegate to specialized Externalizers. For details, read
 * http://infinispan.org/docs/7.0.x/user_guide/user_guide.html#_preassigned_externalizer_id_ranges
 * <p/>
 * The range reserved for the Infinispan Remote Query module is from 1700 to 1799.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@SuppressWarnings("boxing")
public interface ExternalizerIds {

   Integer PROTOBUF_VALUE_WRAPPER = 1700;
   Integer JPA_PROTOBUF_CACHE_EVENT_FILTER_CONVERTER = 1701;
   Integer JPA_PROTOBUF_FILTER_AND_CONVERTER = 1702;
   Integer JPA_CONTINUOUS_QUERY_CACHE_EVENT_FILTER_CONVERTER = 1703;
   Integer JPA_BINARY_PROTOBUF_FILTER_AND_CONVERTER = 1704;
   Integer JPA_CONTINUOUS_QUERY_RESULT = 1705;
}
