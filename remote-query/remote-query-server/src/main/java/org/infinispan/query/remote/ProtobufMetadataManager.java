package org.infinispan.query.remote;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.query.remote.client.ProtobufMetadataManagerMBean;

/**
 * A clustered persistent and replicated repository of protobuf definition files. All protobuf types and their
 * marshallers must be registered with this repository before being used.
 * <p>
 * ProtobufMetadataManager is backed by an internal replicated cache named ___protobuf_metadata.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
@Scope(Scopes.GLOBAL)
public interface ProtobufMetadataManager extends ProtobufMetadataManagerMBean {

   /**
    * @deprecated since 12.1. Will be removed in 15.0. Use the CREATE permission instead.
    */
   @Deprecated
   String SCHEMA_MANAGER_ROLE = "___schema_manager";

   void registerMarshaller(BaseMarshaller<?> marshaller);

   void unregisterMarshaller(BaseMarshaller<?> marshaller);
}
