package org.jgroups.protocols;

import org.infinispan.server.test.util.PartitionHandlingController;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

/**
 * This is custom protocol used for partition handling. What it basically does is, that it discards specific
 * JGroups messages from some allowed nodes. Do not use this class directly (via JGroups Probe), use {@link PartitionHandlingController} instead.
 * <p>
 * The class file is inserted as a JAR into the test servers (see pom.xml).
 *
 * @author Jiri Holusa (jholusa@redhat.com)
 * @author Radim Vansa (rvansa@redhat.com)
 */
@MBean(description = "Enables custom partition handling.")
public class PARTITION_HANDLING extends Protocol {

   private static final short PROTOCOL_ID = (short) 0x51A7;
   private static Log log = LogFactory.getLog(PARTITION_HANDLING.class);

   private int nodeIndex = -1;
   private Set<Integer> allowedNodes;

   private static final String EXECUTED_SUCCESSFULLY = "Executed successfully.";
   private static final String IGNORING_SELFCALL = "Ignoring self-call.";

   static {
      log.info("Registering PARTITION_HANDLING with id " + PROTOCOL_ID);
      ClassConfigurator.add(PROTOCOL_ID, NodeHeader.class);
   }

   public String setAllowedNodes(Set<Integer> allowedNodes) {
      this.allowedNodes = allowedNodes;

      return EXECUTED_SUCCESSFULLY;
   }

   public String addAllowedNode(String server, int index) {
      if (!server.equalsIgnoreCase(getTransport().getLocalAddress())) {
         return IGNORING_SELFCALL;
      }

      if (allowedNodes == null) {
         allowedNodes = new HashSet<>(Arrays.asList(nodeIndex));
      }

      allowedNodes.add(index);
      log.trace("Add new allowed node, index: " + index + " to server " + server);

      return EXECUTED_SUCCESSFULLY;
   }

   public String unsetAllowedNodes(String server) {
      if (!server.equalsIgnoreCase(getTransport().getLocalAddress())) {
         return IGNORING_SELFCALL;
      }

      allowedNodes = null;
      log.trace("Cleared allowed nodes of server " + server);

      return EXECUTED_SUCCESSFULLY;
   }

   public String setNodeIndex(String server, int index) {
      if (!server.equalsIgnoreCase(getTransport().getLocalAddress())) {
         return IGNORING_SELFCALL;
      }

      this.nodeIndex = index;
      log.trace("Index " + index + " set to server " + server);

      return EXECUTED_SUCCESSFULLY;
   }

   @Override
   public Object up(Message msg) {

      NodeHeader header = msg.getHeader(PROTOCOL_ID);
      if (header != null && header.getIndex() < 0) {
         log.trace("Message " + msg.getSrc() + " -> " + msg.getDest() + " with nodeIndex -1");
      } else if (header != null && allowedNodes != null) {
         if (!allowedNodes.contains(header.getIndex())) {
            log.trace("Discarding message " + msg.getSrc() + " -> " + msg.getDest() + " with nodeIndex " + header.getIndex());
            return null;
         }
      }
      return up_prot.up(msg);
   }

   @Override
   public Object down(Message msg) {
      msg.putHeader(PROTOCOL_ID, new NodeHeader(this.nodeIndex));
      return down_prot.down(msg);
   }

   public static class NodeHeader extends Header implements Streamable {
      int index = -1;

      public NodeHeader() {
      }

      public short getMagicId() {
         return (short) 0x51A7;
      }

      public NodeHeader(int index) {
         this.index = index;
      }

      public int getIndex() {
         return index;
      }

      public String toString() {
         return "nodeIndex=" + index;
      }

      public int size() {
         return Global.INT_SIZE;
      }

      public Supplier<? extends org.jgroups.Header> create() {
         return PARTITION_HANDLING.NodeHeader::new;
      }

      public int serializedSize() {
         return 0;
      }

      @Override
      public void writeTo(DataOutput out) throws Exception {
         out.writeInt(index);
      }

      @Override
      public void readFrom(DataInput in) throws Exception {
         index = in.readInt();
      }
   }
}