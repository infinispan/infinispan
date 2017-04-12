package org.infinispan.remoting.transport.jgroups;

import java.util.List;

import org.jgroups.Address;
import org.jgroups.protocols.relay.Route;
import org.jgroups.protocols.relay.SiteMasterPicker;

/**
 * A {@link SiteMasterPicker} implementation that picks the first route.
 * <p>
 * Only a single route can be used in order to keep the data consistent for asynchronous cross-site replication.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class SiteMasterPickerImpl implements SiteMasterPicker {

   @Override
   public Address pickSiteMaster(List<Address> site_masters, Address original_sender) {
      return site_masters.get(0);
   }

   @Override
   public Route pickRoute(String site, List<Route> routes, Address original_sender) {
      return routes.get(0);
   }
}
