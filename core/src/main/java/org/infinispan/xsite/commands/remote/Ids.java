package org.infinispan.xsite.commands.remote;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.xsite.SingleXSiteRpcCommand;

public final class Ids {

   private static final byte START_ID = 0;
   public static final byte STATE_TRANSFER_CONTROL = START_ID;
   public static final byte STATE_TRANSFER_STATE = STATE_TRANSFER_CONTROL + 1;
   public static final byte VISITABLE_COMMAND = STATE_TRANSFER_STATE + 1;
   public static final byte IRAC_UPDATE = VISITABLE_COMMAND + 1;
   public static final byte IRAC_CLEAR = IRAC_UPDATE + 1;
   public static final byte IRAC_TOUCH = IRAC_CLEAR + 1;
   public static final byte IRAC_TOMBSTONE_CHECK = IRAC_TOUCH + 1;
   private static final byte END_ID = IRAC_TOMBSTONE_CHECK + 1;

   private static final Builder[] ID_TO_COMMAND;
   @SuppressWarnings("rawtypes")
   private static final Set<Class<? extends XSiteRequest>> CLASSES;

   static {
      ID_TO_COMMAND = new Builder[END_ID];
      ID_TO_COMMAND[STATE_TRANSFER_CONTROL] = XSiteStateTransferControlRequest::new;
      ID_TO_COMMAND[STATE_TRANSFER_STATE] = XSiteStatePushRequest::new;
      ID_TO_COMMAND[VISITABLE_COMMAND] = SingleXSiteRpcCommand::new;
      ID_TO_COMMAND[IRAC_UPDATE] = IracPutManyRequest::new;
      ID_TO_COMMAND[IRAC_CLEAR] = IracClearKeysRequest::new;
      ID_TO_COMMAND[IRAC_TOUCH] = IracTouchKeyRequest::new;
      ID_TO_COMMAND[IRAC_TOMBSTONE_CHECK] = IracTombstoneCheckRequest::new;

      CLASSES = Collections.unmodifiableSet(checkMappingAndCreateClassSet());
   }

   @SuppressWarnings("rawtypes")
   private static Set<Class<? extends XSiteRequest>> checkMappingAndCreateClassSet() {
      Set<Class<? extends XSiteRequest>> classSet = new HashSet<>();

      // check correct mapping and create class set
      for (byte i = START_ID; i < END_ID; ++i) {
         var cmd = fromId(i);
         if (i != fromId(i).getCommandId()) {
            throw new IllegalStateException("Id does not match. id=" + i + " command=" + fromId(i).getClass());
         }
         classSet.add(cmd.getClass());
      }
      return classSet;
   }

   private Ids() {
   }

   public static XSiteRequest<?> fromId(byte id) {
      return ID_TO_COMMAND[id].build();
   }

   @SuppressWarnings("rawtypes")
   public static Set<Class<? extends XSiteRequest>> getTypeClasses() {
      return CLASSES;
   }

   @FunctionalInterface
   private interface Builder {
      XSiteRequest<?> build();
   }

}
