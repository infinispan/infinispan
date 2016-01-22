package org.infinispan.xsite.status;

/**
 * A site status.
 * <p>
 * A site could be online, offline or none of the previous. In the later case, it is consider in a mixed status and both
 * {@link #isOnline()}  and {@link #isOffline()}  returns {@code false}.
 *
 * @author Pedro Ruivo
 * @since 8.2
 */
public interface SiteStatus {

   /**
    * @return {@code true} if the site is online.
    */
   boolean isOnline();

   /**
    * @return {@code true} if the site is offline.
    */
   boolean isOffline();

}
