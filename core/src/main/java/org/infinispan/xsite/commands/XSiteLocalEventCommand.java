package org.infinispan.xsite.commands;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.GlobalRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.xsite.events.XSiteEvent;
import org.infinispan.xsite.events.XSiteEventsManager;

/**
 * Sends {@link XSiteEvent} list from a local site node.
 *
 * @since 15.0
 */
public class XSiteLocalEventCommand implements GlobalRpcCommand {

   public static final byte COMMAND_ID = 15;
   private List<XSiteEvent> events;

   public XSiteLocalEventCommand() {
   }

   public XSiteLocalEventCommand(List<XSiteEvent> events) {
      this.events = events;
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry globalComponentRegistry) throws Throwable {
      return globalComponentRegistry.getComponent(XSiteEventsManager.class).onLocalEvents(events);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallCollection(events, output, XSiteEvent::writeTo);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      events = MarshallUtil.unmarshallCollection(input, ArrayList::new, XSiteEvent::readFrom);
   }

   @Override
   public String toString() {
      return "XSiteLocalEventCommand{" +
              "events=" + events +
              '}';
   }
}
