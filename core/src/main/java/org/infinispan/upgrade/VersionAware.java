package org.infinispan.upgrade;

import org.infinispan.remoting.transport.NodeVersion;

public interface VersionAware {
   /**
    * Returns a {@link NodeVersion} representing the Infinispan version in which this functionality was added. This value
    * is used to ensure that when the cluster contains different Infinispan versions, only actions compatible with the
    * oldest version are permitted.
    *
    * @return a {@link NodeVersion} corresponding to the Infinispan version this functionality was added.
    */
   NodeVersion supportedSince();
}
