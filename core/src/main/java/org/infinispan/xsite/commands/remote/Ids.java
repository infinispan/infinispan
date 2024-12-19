package org.infinispan.xsite.commands.remote;

public final class Ids {

   private static final byte START_ID = 0;
   public static final byte STATE_TRANSFER_CONTROL = START_ID;
   public static final byte STATE_TRANSFER_STATE = STATE_TRANSFER_CONTROL + 1;
   public static final byte VISITABLE_COMMAND = STATE_TRANSFER_STATE + 1;
   public static final byte IRAC_UPDATE = VISITABLE_COMMAND + 1;
   public static final byte IRAC_CLEAR = IRAC_UPDATE + 1;
   public static final byte IRAC_TOUCH = IRAC_CLEAR + 1;
   public static final byte IRAC_TOMBSTONE_CHECK = IRAC_TOUCH + 1;
   public static final byte SITE_EVENT = IRAC_TOMBSTONE_CHECK + 1;
   private static final byte END_ID = SITE_EVENT + 1;

   private Ids() {
   }
}
