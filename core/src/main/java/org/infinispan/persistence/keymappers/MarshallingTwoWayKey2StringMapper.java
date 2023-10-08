package org.infinispan.persistence.keymappers;

import org.infinispan.commons.marshall.Marshaller;

/**
 *
 * MarshallingTwoWayKey2StringMapper.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface MarshallingTwoWayKey2StringMapper extends TwoWayKey2StringMapper {

   default void setMarshaller(Marshaller marshaller) {
      // no-op
   }
}
