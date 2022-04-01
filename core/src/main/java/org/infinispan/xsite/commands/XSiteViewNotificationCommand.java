package org.infinispan.xsite.commands;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.GlobalRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.xsite.GlobalXSiteAdminOperations;

/**
 * A {@link GlobalRpcCommand} which notifies new remote sites are online.
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
public class XSiteViewNotificationCommand implements GlobalRpcCommand {

   public static final int COMMAND_ID = 34;

   private Collection<String> sitesUp;

   public XSiteViewNotificationCommand() {
      this(Collections.emptyList());
   }

   public XSiteViewNotificationCommand(Collection<String> sitesUp) {
      this.sitesUp = sitesUp;
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
      MarshallUtil.marshallCollection(sitesUp, output, DataOutput::writeUTF);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      sitesUp = MarshallUtil.unmarshallCollection(input, ArrayList::new, DataInput::readUTF);
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry globalComponentRegistry) throws Throwable {
      globalComponentRegistry.getComponent(GlobalXSiteAdminOperations.class).onSitesUp(sitesUp);
      return CompletableFutures.completedNull();
   }
}
