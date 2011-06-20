package org.jboss.seam.infinispan.event.cachemanager;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;

import javax.enterprise.event.Event;
import java.util.List;

@Listener
public class ViewChangedAdapter extends AbstractAdapter<ViewChangedEvent> {

   public static final ViewChangedEvent EMPTY = new ViewChangedEvent() {

      public Type getType() {
         return null;
      }

      public EmbeddedCacheManager getCacheManager() {
         return null;
      }

      public List<Address> getNewMembers() {
         return null;
      }

      public List<Address> getOldMembers() {
         return null;
      }

      public Address getLocalAddress() {
         return null;
      }

      public boolean isNeedsToRejoin() {
         return false;
      }

      public int getViewId() {
         return 0;
      }

      @Override
      public boolean isMergeView() {
         return false;
      }
   };

   public ViewChangedAdapter(Event<ViewChangedEvent> event) {
      super(event);
   }

   @ViewChanged
   public void fire(ViewChangedEvent payload) {
      super.fire(payload);
   }

}
