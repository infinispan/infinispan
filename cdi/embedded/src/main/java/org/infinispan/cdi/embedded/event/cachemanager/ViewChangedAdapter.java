package org.infinispan.cdi.embedded.event.cachemanager;

import java.util.Collections;
import java.util.List;

import javax.enterprise.event.Event;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;

/**
 * @author Pete Muir
 */
@Listener
public class ViewChangedAdapter extends AbstractAdapter<ViewChangedEvent> {

   public static final ViewChangedEvent EMPTY = new ViewChangedEvent() {

      @Override
      public Type getType() {
         return null;
      }

      @Override
      public EmbeddedCacheManager getCacheManager() {
         return null;
      }

      @Override
      public List<Address> getNewMembers() {
         return Collections.emptyList();
      }

      @Override
      public List<Address> getOldMembers() {
         return Collections.emptyList();
      }

      @Override
      public Address getLocalAddress() {
         return null;
      }

      public boolean isNeedsToRejoin() {
         return false;
      }

      @Override
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

   @Override
   @ViewChanged
   public void fire(ViewChangedEvent payload) {
      super.fire(payload);
   }
}
