package org.infinispan.xsite.response;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.responses.Response;
import org.infinispan.xsite.commands.XSiteAutoTransferStatusCommand;

/**
 * A {@link Response} implementation for command {@link XSiteAutoTransferStatusCommand}.
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
public class AutoStateTransferResponse implements Response {

   public static final AbstractExternalizer<AutoStateTransferResponse> EXTERNALIZER = new Externalizer();
   private static final XSiteStateTransferMode[] CACHED_VALUES = XSiteStateTransferMode.values();
   private final boolean isOffline;
   private final XSiteStateTransferMode stateTransferMode;

   public AutoStateTransferResponse(boolean isOffline, XSiteStateTransferMode stateTransferMode) {
      this.isOffline = isOffline;
      this.stateTransferMode = stateTransferMode;
   }

   private static XSiteStateTransferMode valueOf(int ordinal) {
      return CACHED_VALUES[ordinal];
   }

   @Override
   public boolean isSuccessful() {
      return true;
   }

   @Override
   public boolean isValid() {
      return true;
   }

   public boolean isOffline() {
      return isOffline;
   }

   public XSiteStateTransferMode stateTransferMode() {
      return stateTransferMode;
   }

   public boolean canDoAutomaticStateTransfer() {
      return isOffline && stateTransferMode == XSiteStateTransferMode.AUTO;
   }

   private static class Externalizer extends AbstractExternalizer<AutoStateTransferResponse> {
      @Override
      public void writeObject(ObjectOutput output, AutoStateTransferResponse response) throws IOException {
         output.writeBoolean(response.isOffline);
         MarshallUtil.marshallEnum(response.stateTransferMode, output);
      }

      @Override
      public AutoStateTransferResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new AutoStateTransferResponse(input.readBoolean(),
               MarshallUtil.unmarshallEnum(input, AutoStateTransferResponse::valueOf));
      }

      @Override
      public Integer getId() {
         return Ids.XSITE_AUTO_TRANSFER_RESPONSE;
      }

      @Override
      public Set<Class<? extends AutoStateTransferResponse>> getTypeClasses() {
         return Collections.singleton(AutoStateTransferResponse.class);
      }
   }
}
