package org.infinispan.xsite.status;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Abstract class to create mixed {@link SiteStatus}.
 * <p>
 * Mixed {@link SiteStatus} are status in which some considers the site to be online and other to be offline.
 *
 * @author Pedro Ruivo
 * @since 8.2
 */
public abstract class AbstractMixedSiteStatus<E> implements SiteStatus {

   protected final List<E> online;
   protected final List<E> offline;

   protected AbstractMixedSiteStatus(Collection<E> online, Collection<E> offline) {
      this.online = toImmutable(online);
      this.offline = toImmutable(offline);
   }

   protected static <E> List<E> toImmutable(Collection<E> collection) {
      return Collections.unmodifiableList(new ArrayList<>(collection));
   }

   @Override
   public final boolean isOnline() {
      return false;
   }

   @Override
   public final boolean isOffline() {
      return false;
   }

   public List<E> getOnline() {
      return online;
   }

   public List<E> getOffline() {
      return offline;
   }
}
