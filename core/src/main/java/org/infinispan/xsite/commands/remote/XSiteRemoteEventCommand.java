package org.infinispan.xsite.commands.remote;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.xsite.events.XSiteEvent;
import org.infinispan.xsite.events.XSiteEventsManager;

/**
 * Sends a {@link XSiteEvent} list from a remote site.
 *
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_REMOTE_EVENT_COMMAND)
public class XSiteRemoteEventCommand implements XSiteRequest<Void> {

   @ProtoField(number = 1, collectionImplementation = ArrayList.class)
   List<XSiteEvent> events;

   @ProtoFactory
   public XSiteRemoteEventCommand(List<XSiteEvent> events) {
      this.events = events;
   }

   @Override
   public CompletionStage<Void> invokeInLocalSite(String origin, GlobalComponentRegistry registry) {
      return registry.getComponent(XSiteEventsManager.class).onRemoteEvents(events);
   }

   @Override
   public byte getCommandId() {
      return Ids.SITE_EVENT;
   }

   @Override
   public String toString() {
      return "XSiteRemoteEventCommand{" +
            "events=" + events +
            '}';
   }
}
