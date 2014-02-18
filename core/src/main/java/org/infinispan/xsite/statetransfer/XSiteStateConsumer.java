package org.infinispan.xsite.statetransfer;

/**
 * It contains the logic needed to consume the state sent from other site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public interface XSiteStateConsumer {

   /**
    * It notifies the start of state transfer from other site.
    */
   public void startStateTransfer();

   /**
    * It notifies the end of state transfer from other site.
    */
   public void endStateTransfer();

   /**
    * It applies state from other site.
    *
    * @param chunk a chunk of keys
    * @throws Exception if something go wrong while applying the state
    */
   public void applyState(XSiteState[] chunk) throws Exception;
}
