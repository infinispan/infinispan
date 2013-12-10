package org.infinispan.server.hotrod

import logging.Log

/**
 * Version specific encoders are included here.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
object Encoders {

   /**
    * Encoder for version 1.0 of the Hot Rod protocol.
    */
   object Encoder10 extends AbstractEncoder1x

   /**
    * Encoder for version 1.1 of the Hot Rod protocol.
    */
   object Encoder11 extends AbstractTopologyAwareEncoder1x with Log

   /**
    * Encoder for version 1.2 of the Hot Rod protocol.
    */
   object Encoder12 extends AbstractTopologyAwareEncoder1x with Log

   /**
    * Encoder for version 1.3 of the Hot Rod protocol.
    */
   object Encoder13 extends AbstractTopologyAwareEncoder1x with Log
}
