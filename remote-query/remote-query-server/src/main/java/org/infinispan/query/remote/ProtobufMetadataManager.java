package org.infinispan.query.remote;

import javax.management.ObjectName;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.query.remote.client.ProtobufMetadataManagerMBean;

/**
 * A clustered repository of protobuf definition files. All protobuf types and their marshallers must be registered with
 * this repository before being used.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
@Scope(Scopes.GLOBAL)
public interface ProtobufMetadataManager extends ProtobufMetadataManagerMBean {

   String SCHEMA_MANAGER_ROLE = "___schema_manager";

   ObjectName getObjectName();

   void registerMarshaller(BaseMarshaller<?> marshaller);

   void unregisterMarshaller(BaseMarshaller<?> marshaller);
}
