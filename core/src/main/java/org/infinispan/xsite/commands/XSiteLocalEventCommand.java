package org.infinispan.xsite.commands;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.GlobalRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.xsite.events.XSiteEvent;
import org.infinispan.xsite.events.XSiteEventsManager;

/**
 * Sends {@link XSiteEvent} list from a local site node.
 *
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_LOCAL_EVENT_COMMAND)
public class XSiteLocalEventCommand implements GlobalRpcCommand {

   public static final byte COMMAND_ID = 2;

   @ProtoField(1)
   List<XSiteEvent> events;

   @ProtoFactory
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
   public String toString() {
      return "XSiteLocalEventCommand{" +
            "events=" + events +
            '}';
   }
}
