package org.infinispan.commons.dataconversion;

import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;

/**
 * @since 9.1
 */
public class GenericJbossMarshallerEncoder extends MarshallerEncoder {

   public GenericJbossMarshallerEncoder() {
      super(new GenericJBossMarshaller());
   }

}
