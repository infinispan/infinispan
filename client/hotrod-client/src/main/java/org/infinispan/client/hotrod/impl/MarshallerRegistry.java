package org.infinispan.client.hotrod.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;

/**
 * A registry of {@link Marshaller} along with its {@link MediaType}.
 *
 * @since 9.3
 */
public final class MarshallerRegistry {

   public static final Log log = LogFactory.getLog(MarshallerRegistry.class, Log.class);

   private final Map<MediaType, Marshaller> marshallerByMediaType = new ConcurrentHashMap<>();

   public void registerMarshaller(Marshaller marshaller) {
      marshallerByMediaType.put(marshaller.mediaType().withoutParameters(), marshaller);
   }

   public Marshaller getMarshaller(MediaType mediaType) {
      return marshallerByMediaType.get(mediaType);
   }

}
