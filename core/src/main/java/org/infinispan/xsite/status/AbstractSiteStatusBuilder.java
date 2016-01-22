package org.infinispan.xsite.status;

import java.util.LinkedList;
import java.util.List;

/**
 * A {@link SiteStatus} builder based on its online and offline members.
 *
 * @author Pedro Ruivo
 * @since 8.2
 */
public abstract class AbstractSiteStatusBuilder<E> {

   private final List<E> onlineElements;
   private final List<E> offlineElements;

   protected AbstractSiteStatusBuilder() {
      offlineElements = new LinkedList<>();
      onlineElements = new LinkedList<>();
   }

   /**
    * Adds the element with an online connection to the site.
    *
    * @param member The member.
    */
   public final void onlineOn(E member) {
      onlineElements.add(member);
   }

   /**
    * Adds the member with an offline connection to the site.
    *
    * @param member The member.
    */
   public final void offlineOn(E member) {
      offlineElements.add(member);
   }

   /**
    * @return {@link SiteStatus} created.
    */
   public final SiteStatus build() {
      if (isOnline()) {
         return OnlineSiteStatus.getInstance();
      } else if (isOffline()) {
         return OfflineSiteStatus.getInstance();
      } else {
         return createMixedStatus(onlineElements, offlineElements);
      }
   }

   protected boolean isOnline() {
      return !onlineElements.isEmpty() && offlineElements.isEmpty();
   }

   protected boolean isOffline() {
      return onlineElements.isEmpty() && !offlineElements.isEmpty();
   }

   protected abstract SiteStatus createMixedStatus(List<E> onlineElements, List<E> offlineElements);

}
