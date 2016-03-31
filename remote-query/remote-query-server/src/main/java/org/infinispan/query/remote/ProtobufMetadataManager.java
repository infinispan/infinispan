package org.infinispan.query.remote;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.query.remote.client.ProtobufMetadataManagerMBean;

import javax.management.ObjectName;

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

   void setObjectName(ObjectName objectName);

   void registerMarshaller(BaseMarshaller<?> marshaller);
}
