package org.infinispan.query.remote.impl.filter;

import java.util.Map;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.core.impl.QueryCache;
import org.infinispan.query.core.impl.eventfilter.IckleFilterAndConverter;
import org.infinispan.query.remote.impl.RemoteQueryManager;

/**
 * A subclass of {@link IckleFilterAndConverter} that is able to deal with binary protobuf values wrapped in a
 * ProtobufValueWrapper.
 *
 * @author anistor@redhat.com
 * @since 7.2
 */
@ProtoTypeId(ProtoStreamTypeIds.REMOTE_QUERY_ICKLE_PROTOBUF_FILTER_AND_CONVERTER)
public final class IckleProtobufFilterAndConverter extends IckleFilterAndConverter<Object, Object> {

   public IckleProtobufFilterAndConverter(String queryString, Map<String, Object> namedParameters) {
      super(queryString, namedParameters, ProtobufMatcher.class);
   }

   @ProtoFactory
   IckleProtobufFilterAndConverter(String queryString, MarshallableMap<String, Object> wrappedNamedParameters, Class<? extends Matcher> matcherImplClass) {
      super(queryString, wrappedNamedParameters, matcherImplClass);
   }

   @Override
   protected void injectDependencies(ComponentRegistry componentRegistry, QueryCache queryCache) {
      RemoteQueryManager remoteQueryManager = componentRegistry.getComponent(RemoteQueryManager.class);
      matcherImplClass = remoteQueryManager.getMatcherClass(MediaType.APPLICATION_PROTOSTREAM);
      super.injectDependencies(componentRegistry, queryCache);
   }
}
