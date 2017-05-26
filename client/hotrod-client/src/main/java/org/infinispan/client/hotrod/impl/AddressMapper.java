package org.infinispan.client.hotrod.impl;

import java.net.SocketAddress;

/**
 * A class used for mapping internal into external (and vice versa) addresses.
 *
 * <p>
 *    This approach is helpful if the platform hosting Hot Rod Server uses different address space for
 *    accessing data internally/externally.
 * </p>
 */
public interface AddressMapper {

   /**
    * Returns external address based on internal one.
    *
    * <p>
    *    The default implementation returns provided address without any change.
    * </p>
    *
    * @param internalAddress Internal address.
    * @return External Address based on internal address.
    */
   default SocketAddress toExternalAddress(SocketAddress internalAddress) {
      return internalAddress;
   }

   /**
    * Returns internal address based on external one.
    *
    * <p>
    *    The default implementation returns provided address without any change.
    * </p>
    *
    * @param externalAddress External address.
    * @return Internal address based on external address.
    */
   default SocketAddress toInternalAddress(SocketAddress externalAddress) {
      return externalAddress;
   }

}
