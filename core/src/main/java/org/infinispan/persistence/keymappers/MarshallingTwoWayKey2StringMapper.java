package org.infinispan.persistence.keymappers;

import org.infinispan.commons.marshall.StreamingMarshaller;

/**
 *
 * MarshallingTwoWayKey2StringMapper.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface MarshallingTwoWayKey2StringMapper extends TwoWayKey2StringMapper {
   void setMarshaller(StreamingMarshaller marshaller);
}
