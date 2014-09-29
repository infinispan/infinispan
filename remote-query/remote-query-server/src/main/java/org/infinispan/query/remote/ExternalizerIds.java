package org.infinispan.query.remote;

/**
 * Identifiers used by the Marshaller to delegate to specialized Externalizers. For details, read
 * http://infinispan.org/docs/7.0.x/user_guide/user_guide.html#_preassigned_externalizer_id_ranges
 * <p/>
 * The range reserved for the Infinispan Remote Query module is from 1700 to 1799.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface ExternalizerIds {

   Integer PROTOBUF_VALUE_WRAPPER = 1700;
}
