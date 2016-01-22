package org.infinispan.xsite.status;

/**
 * {@link SiteStatus} implementation for online sites.
 *
 * This class is a singleton and its instance is accessible via {@link #getInstance()}.
 *
 * @author Pedro Ruivo
 * @since 8.2
 */
public class OnlineSiteStatus implements SiteStatus {


   private OnlineSiteStatus() {
   }

   public static OnlineSiteStatus getInstance() {
      return SingletonHolder.INSTANCE;
   }

   @Override
   public boolean isOnline() {
      return true;
   }

   @Override
   public boolean isOffline() {
      return false;
   }

   private static class SingletonHolder {
      private static final OnlineSiteStatus INSTANCE = new OnlineSiteStatus();
   }
}
