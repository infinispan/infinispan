package org.infinispan.remoting.transport.jgroups;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.test.AbstractInfinispanTest;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.protocols.TP;
import org.jgroups.util.AsciiString;
import org.jgroups.util.Bits;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "remoting.transport.jgroups.BlockingBundlerTest")
public class BlockingBundlerTest extends AbstractInfinispanTest {
   private static final AsciiString CLUSTER_NAME = new AsciiString("CLUSTER");
   private static final int MAX_BUNDLE_SIZE = 1000;
   private static final byte[] BYTES_100 = new byte[100];
   private static final byte[] BYTES_500 = new byte[500];

   BlockingBundler bundler;
   private MockTP transport;

   @BeforeMethod
   public void setUp() throws Exception {
      bundler = new BlockingBundler();
      transport = new MockTP();
      Util.setField(Util.getField(TP.class, "cluster_name"), transport, CLUSTER_NAME);
      transport.setMaxBundleSize(MAX_BUNDLE_SIZE);
      transport.setBundler(bundler);
      transport.init();
      bundler.init(transport);
   }

   public void testMulticast1() throws Exception {
      bundler.send(new Message(null, BYTES_100));

      assertTrue(bundler.sendQueued());

      Message message = transport.pollMulticast();
      assertMulticastMessage(message, BYTES_100);
   }

   public void testMulticast2() throws Exception {
      bundler.send(new Message(null, BYTES_500));
      bundler.send(new Message(null, BYTES_100));

      assertTrue(bundler.sendQueued());

      List<Message> messages = transport.pollMulticastBatch();
      Message message1 = messages.get(0);
      assertMulticastMessage(message1, BYTES_500);
      Message message2 = messages.get(1);
      assertMulticastMessage(message2, BYTES_100);
   }

   public void testMulticast3Full() throws Exception {
      bundler.send(new Message(null, BYTES_500));
      bundler.send(new Message(null, BYTES_500));

      assertTrue(bundler.sendQueued());

      Message message1 = transport.pollMulticast();
      assertMulticastMessage(message1, BYTES_500);

      assertTrue(bundler.sendQueued());

      Message message2 = transport.pollMulticast();
      assertMulticastMessage(message2, BYTES_500);
   }

   public void testMulticast4Block() throws Exception {
      bundler.send(new Message(null, BYTES_500));
      bundler.send(new Message(null, BYTES_500));

      // Current bundle and serialization buffer are full, the 3rd send blocks
      Future<Void> send3Future = fork(() -> bundler.send(new Message(null, BYTES_500)));
      Thread.sleep(10);
      assertFalse(send3Future.isDone());

      // Empty the serialization buffer and unblock the 3rd send
      assertTrue(bundler.sendQueued());

      Message message1 = transport.pollMulticast();
      assertMulticastMessage(message1, BYTES_500);

      send3Future.get(10, TimeUnit.SECONDS);

      // Current bundle and serialization buffer are again full
      assertTrue(bundler.sendQueued());

      Message message2 = transport.pollMulticast();
      assertMulticastMessage(message2, BYTES_500);

      assertTrue(bundler.sendQueued());

      Message message3 = transport.pollMulticast();
      assertMulticastMessage(message3, BYTES_500);
   }

   public void testMulticast5BlockReorder() throws Exception {
      bundler.send(new Message(null, BYTES_500));
      bundler.send(new Message(null, BYTES_500));

      // Current bundle and serialization buffer are full, the 3rd send blocks
      Future<Void> send3Future = fork(() -> bundler.send(new Message(null, BYTES_500)));
      Thread.sleep(10);
      assertFalse(send3Future.isDone());

      // A smaller message fits into the current bundle
      bundler.send(new Message(null, BYTES_100));

      // Empty the serialization buffer and unblock the 3rd send
      assertTrue(bundler.sendQueued());

      Message message1 = transport.pollMulticast();
      assertMulticastMessage(message1, BYTES_500);

      send3Future.get(10, TimeUnit.SECONDS);

      // Current bundle and serialization buffer are again full
      assertTrue(bundler.sendQueued());

      List<Message> batch = transport.pollMulticastBatch();
      Message message2 = batch.get(0);
      assertMulticastMessage(message2, BYTES_500);
      Message message3 = batch.get(1);
      assertMulticastMessage(message3, BYTES_100);

      assertTrue(bundler.sendQueued());

      Message message4 = transport.pollMulticast();
      assertMulticastMessage(message4, BYTES_500);
   }

   private void assertMulticastMessage(Message message, byte[] payload) {
      assertNull(message.getDest());
      assertEquals(payload, message.buffer());
   }
}

class MockTP extends TP {
   public static short ID = 1;
   private Queue<Object> multicasts = new LinkedBlockingQueue<>();
   private Queue<Object> unicasts = new LinkedBlockingQueue<>();

   public Message pollMulticast() {
      return (Message) multicasts.poll();
   }

   public List<Message> pollMulticastBatch() {
      return (List<Message>) multicasts.poll();
   }

   public Message pollUnicastMessage() {
      return (Message) unicasts.poll();
   }

   public List<Message> pollUnicastBatch() {
      return (List<Message>) unicasts.poll();
   }

   @Override
   public boolean supportsMulticasting() {
      return true;
   }

   @Override
   public void sendMulticast(byte[] data, int offset, int length) throws Exception {
      multicasts.add(parseBuffer(data, offset, length));
   }

   @Override
   public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
      unicasts.add(parseBuffer(data, offset, length));
   }

   @Override
   public String getInfo() {
      return null;
   }

   @Override
   protected PhysicalAddress getPhysicalAddress() {
      return null;
   }

   public Object parseBuffer(byte[] data, int offset, int length) throws Exception {
      // the length of a message needs to be at least 3 bytes: version (2) and flags (1) // JGRP-2210
      if (length < Global.SHORT_SIZE + Global.BYTE_SIZE)
         throw new IllegalArgumentException();

      short version = Bits.readShort(data, offset);
      if (!versionMatch(version, null))
         throw new IllegalArgumentException();
      offset += Global.SHORT_SIZE;
      byte flags = data[offset];
      offset += Global.BYTE_SIZE;

      boolean is_message_list = (flags & LIST) == LIST, multicast = (flags & MULTICAST) == MULTICAST;
      ByteArrayDataInputStream in = new ByteArrayDataInputStream(data, offset, length);
      if (is_message_list) // used if message bundling is enabled
         return Util.readMessageList(in, ID);
      else
         return Util.readMessage(in);
   }
}
