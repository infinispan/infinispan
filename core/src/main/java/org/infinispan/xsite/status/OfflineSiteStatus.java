package org.infinispan.xsite.status;

/**
 * {@link SiteStatus} implementation for offline sites.
 *
 * This class is a singleton and its instance is accessible via {@link #getInstance()}.
 *
 * @author Pedro Ruivo
 * @since 8.2
 */
public class OfflineSiteStatus implements SiteStatus {


   private OfflineSiteStatus() {
   }

   public static OfflineSiteStatus getInstance() {
      return SingletonHolder.INSTANCE;
   }

   @Override
   public boolean isOnline() {
      return false;
   }

   @Override
   public boolean isOffline() {
      return true;
   }

   private static class SingletonHolder {
      private static final OfflineSiteStatus INSTANCE = new OfflineSiteStatus();
   }
}
