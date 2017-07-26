package org.infinispan.marshall.core;

import java.util.Map;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * @see EncoderRegistry
 * @since 9.1
 */
@Scope(Scopes.GLOBAL)
public class EncoderRegistryImpl implements EncoderRegistry {

   private final Map<Class<? extends Encoder>, Encoder> encoderMap = CollectionFactory.makeConcurrentMap();
   private final Map<Class<? extends Wrapper>, Wrapper> wrapperMap = CollectionFactory.makeConcurrentMap();

   @Override
   public void registerEncoder(Encoder encoder) {
      encoderMap.put(encoder.getClass(), encoder);
   }

   @Override
   public void registerWrapper(Wrapper wrapper) {
      wrapperMap.put(wrapper.getClass(), wrapper);
   }

   @Override
   public Encoder getEncoder(Class<? extends Encoder> clazz) {
      Encoder encoder = encoderMap.get(clazz);
      if (encoder == null) {
         throw new EncodingException("Encoder not found: " + clazz);
      }
      return encoder;
   }

   @Override
   public Wrapper getWrapper(Class<? extends Wrapper> wrapperClass) {
      return wrapperMap.get(wrapperClass);
   }

}
