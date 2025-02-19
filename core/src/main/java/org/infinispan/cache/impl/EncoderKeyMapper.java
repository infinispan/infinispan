package org.infinispan.cache.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * {@link java.util.function.Function} that uses a keyEncoder to converter keys from the configured storage format to
 * the requested format.
 *
 * @since 9.1
 */
@ProtoTypeId(ProtoStreamTypeIds.ENCODER_KEY_MAPPER)
@Scope(Scopes.NONE)
public class EncoderKeyMapper<K> implements EncodingFunction<K> {

   @ProtoField(1)
   final DataConversion dataConversion;

   @ProtoFactory
   public EncoderKeyMapper(DataConversion dataConversion) {
      this.dataConversion = dataConversion;
   }

   @Inject
   public void injectDependencies(ComponentRegistry registry) {
      registry.wireDependencies(dataConversion);
   }

   @Override
   @SuppressWarnings("unchecked")
   public K apply(K k) {
      return (K) dataConversion.fromStorage(k);
   }
}
