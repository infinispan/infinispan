package org.infinispan.query.remote;

import org.infinispan.commons.internal.InternalCacheNames;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.query.remote.client.ProtobufMetadataManagerMBean;

/**
 * A clustered persistent and replicated repository of protobuf definition files. All protobuf types and their
 * marshallers must be registered with this repository before being used.
 * <p>
 * ProtobufMetadataManager is backed by an internal replicated cache named {@link InternalCacheNames#PROTOBUF_METADATA_CACHE_NAME}.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
@Scope(Scopes.GLOBAL)
public interface ProtobufMetadataManager extends ProtobufMetadataManagerMBean {

   void registerMarshaller(BaseMarshaller<?> marshaller);

   void unregisterMarshaller(BaseMarshaller<?> marshaller);
}
