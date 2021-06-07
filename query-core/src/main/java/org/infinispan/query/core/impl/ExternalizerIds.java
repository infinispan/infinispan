package org.infinispan.query.core.impl;

//TODO [anistor] assign proper range
/**
 * Identifiers used by the Marshaller to delegate to specialized Externalizers. For details, read
 * https://infinispan.org/docs/10.0.x/user_guide/user_guide.html#preassigned_externalizer_id_ranges
 * <p>
 * The range reserved for the Infinispan Query Core module is from 1600 to 1699.
 *
 * @author anistor@redhat.com
 * @since 10.1
 */
public interface ExternalizerIds {

   Integer ICKLE_FILTER_AND_CONVERTER = 1600;

   Integer ICKLE_FILTER_RESULT = 1611;

   Integer ICKLE_CACHE_EVENT_FILTER_CONVERTER = 1614;

   Integer ICKLE_CONTINUOUS_QUERY_CACHE_EVENT_FILTER_CONVERTER = 1616;

   Integer ICKLE_CONTINUOUS_QUERY_RESULT = 1617;

   Integer ICKLE_DELETE_FUNCTION = 1618;
}
