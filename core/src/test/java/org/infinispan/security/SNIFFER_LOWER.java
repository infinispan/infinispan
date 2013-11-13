package org.infinispan.security;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.ENCRYPT;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * Class SNIFFER_LOWER is new helper Protocol to inject below ENCRYPT protocol in JGroups configuration file. After the
 * decrypted message was passed through ProtocolStack it's copied and will used in SNIFFER_UPPER protocol
 */
public class SNIFFER_LOWER extends Protocol {

   private static short encryptID;
   static Message messageUP; // use this in SNIFFER_UPPER protocol
   static int msgCounter;

   static {
      ClassConfigurator.add((short) 1025, SNIFFER_LOWER_HEADER.class);
      ClassConfigurator.addProtocol((short) 1025, SNIFFER_LOWER.class);
      encryptID = ClassConfigurator.getProtocolId(ENCRYPT.class);
      msgCounter = 0;
   }

   public SNIFFER_LOWER() {
      name = getClass().getSimpleName();
   }

   public Object down(Event evt) {
      if (evt.getType() == Event.MSG) {
         Message msg = (Message) evt.getArg();
         if (msg.getHeader(encryptID) != null) {
            if (log.isTraceEnabled())
               log.trace(String.format("down(): %d bytes", msg.getLength()));
            SNIFFER_LOWER_HEADER hdr = new SNIFFER_LOWER_HEADER();
            Message copy = msg.copy();
            copy.putHeader(this.id, hdr);
            return down_prot.down(new Event(Event.MSG, copy));
         }
      }
      return down_prot.down(evt);
   }

   public Object up(Event evt) {
      if (evt.getType() == Event.MSG) {
         Message msg = (Message) evt.getArg();
         if (msg.getHeader(encryptID) != null) {
            messageUP = msg.copy();
            msgCounter++;
            if (log.isTraceEnabled())
               log.trace(String.format("up(): %d bytes", msg.getLength()));
            return up_prot.up(new Event(Event.MSG, msg));
         }
      }
      return up_prot.up(evt);
   }

   public void up(MessageBatch batch) {
      for (Message msg : batch) {
         if (msg.getHeader(encryptID) != null) {
            if (log.isTraceEnabled())
               log.trace(String.format("up() batch: %d bytes", msg.getLength()));
         }
      }
      if (!batch.isEmpty())
         up_prot.up(batch);
   }

   public static class SNIFFER_LOWER_HEADER extends org.jgroups.Header {

      @Override
      public int size() {
         return 0;
      }

      @Override
      public void writeTo(DataOutput out) throws Exception {
         // leave it empty as we don't have some extra attributes to be written
      }

      @Override
      public void readFrom(DataInput in) throws Exception {
         // leave it empty as we dont have some extra atrributes to be read
      }
   }
}


