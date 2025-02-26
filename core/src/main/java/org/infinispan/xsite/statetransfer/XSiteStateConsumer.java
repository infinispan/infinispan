package org.infinispan.xsite.statetransfer;

import java.util.List;

/**
 * It contains the logic needed to consume the state sent from other site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public interface XSiteStateConsumer {

   /**
    * It notifies the start of state transfer from other site.
    *
    * @param sendingSite the site name that will send the state.
    * @throws org.infinispan.commons.CacheException if this node is received state from a different site name.
    */
   void startStateTransfer(String sendingSite);

   /**
    * It notifies the end of state transfer from other site.
    *
    * @param sendingSite the site name that is sending the state.
    */
   void endStateTransfer(String sendingSite);

   /**
    * It applies state from other site.
    *
    * @param chunk a chunk of keys
    * @throws Exception if something go wrong while applying the state
    */
   void applyState(List<XSiteState> chunk) throws Exception;

   /**
    * @return the site name that is sending the state.
    */
   String getSendingSiteName();
}
