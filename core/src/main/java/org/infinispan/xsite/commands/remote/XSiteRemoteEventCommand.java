package org.infinispan.xsite.commands.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.xsite.events.XSiteEvent;
import org.infinispan.xsite.events.XSiteEventsManager;

/**
 * Sends a {@link XSiteEvent} list from a remote site.
 *
 * @since 15.0
 */
public class XSiteRemoteEventCommand implements XSiteRequest<Void> {

   private List<XSiteEvent> events;

   public XSiteRemoteEventCommand() {
   }

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
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallCollection(events, output, XSiteEvent::writeTo);
   }

   @Override
   public XSiteRequest<Void> readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      events = MarshallUtil.unmarshallCollection(input, ArrayList::new, XSiteEvent::readFrom);
      return this;
   }

   @Override
   public String toString() {
      return "XSiteRemoteEventCommand{" +
            "events=" + events +
            '}';
   }
}
