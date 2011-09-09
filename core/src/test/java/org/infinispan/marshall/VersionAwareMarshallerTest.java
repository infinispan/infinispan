/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.marshall;

import static org.infinispan.test.TestingUtil.*;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.Cache;
import org.infinispan.atomic.AtomicHashMap;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.control.RehashControlCommand;
import org.infinispan.commands.control.StateTransferControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.entries.*;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.jboss.JBossMarshallingTest.CustomReadObjectMethod;
import org.infinispan.marshall.jboss.JBossMarshallingTest.ObjectThatContainsACustomReadObjectMethod;
import org.infinispan.remoting.MIMECacheEntry;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.ExtendedResponse;
import org.infinispan.remoting.responses.RequestIgnoredResponse;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionLog;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.ByteArrayKey;
import org.infinispan.util.FastCopyHashMap;
import org.infinispan.util.Immutables;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.hash.MurmurHash2;
import org.infinispan.util.hash.MurmurHash3;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.marshalling.TraceInformation;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.UUID;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "marshall.VersionAwareMarshallerTest")
public class VersionAwareMarshallerTest extends AbstractInfinispanTest {

   private static final Log log = LogFactory.getLog(VersionAwareMarshallerTest.class);
   private AbstractDelegatingMarshaller marshaller;
   private EmbeddedCacheManager cm;

   private final TransactionFactory gtf = new TransactionFactory();

   public VersionAwareMarshallerTest() {
      gtf.init(false, false, true);
   }

   @BeforeTest
   public void setUp() {
      // Use a clustered cache manager to be able to test global marshaller interaction too
      cm = TestCacheManagerFactory.createClusteredCacheManager();
      cm.getDefaultConfiguration().fluent().clustering().mode(Configuration.CacheMode.DIST_SYNC);
      marshaller = extractCacheMarshaller(cm.getCache());
   }

   @AfterClass
   public void tearDown() {
      cm.stop();
   }

   public void testJGroupsAddressMarshalling() throws Exception {
      JGroupsAddress address = new JGroupsAddress(new IpAddress(12345));
      marshallAndAssertEquality(address);
   }

   public void testGlobalTransactionMarshalling() throws Exception {
      JGroupsAddress jGroupsAddress = new JGroupsAddress(new IpAddress(12345));
      GlobalTransaction gtx = gtf.newGlobalTransaction(jGroupsAddress, false);
      marshallAndAssertEquality(gtx);
   }

   public void testListMarshalling() throws Exception {
      List l1 = new ArrayList();
      List l2 = new LinkedList();
      for (int i = 0; i < 10; i++) {
         JGroupsAddress jGroupsAddress = new JGroupsAddress(new IpAddress(1000 * i));
         GlobalTransaction gtx = gtf.newGlobalTransaction(jGroupsAddress, false);
         l1.add(gtx);
         l2.add(gtx);
      }
      marshallAndAssertEquality(l1);
      marshallAndAssertEquality(l2);
   }

   public void testMapMarshalling() throws Exception {
      Map m1 = new HashMap();
      Map m2 = new TreeMap();
      Map m3 = new HashMap();
      Map<Integer, GlobalTransaction> m4 = new FastCopyHashMap<Integer, GlobalTransaction>();
      for (int i = 0; i < 10; i++) {
         JGroupsAddress jGroupsAddress = new JGroupsAddress(new IpAddress(1000 * i));
         GlobalTransaction gtx = gtf.newGlobalTransaction(jGroupsAddress, false);
         m1.put(1000 * i, gtx);
         m2.put(1000 * i, gtx);
         m4.put(1000 * i, gtx);
      }
      Map m5 = Immutables.immutableMapWrap(m3);
      marshallAndAssertEquality(m1);
      marshallAndAssertEquality(m2);
      byte[] bytes = marshaller.objectToByteBuffer(m4);
      Map<Integer, GlobalTransaction> m4Read = (Map<Integer, GlobalTransaction>) marshaller.objectFromByteBuffer(bytes);
      for (Map.Entry<Integer, GlobalTransaction> entry : m4.entrySet()) {
         assert m4Read.get(entry.getKey()).equals(entry.getValue()) : "Writen[" + entry.getValue() + "] and read[" + m4Read.get(entry.getKey()) + "] objects should be the same";
      }

      marshallAndAssertEquality(m5);
   }

   public void testSetMarshalling() throws Exception {
      Set s1 = new HashSet();
      Set s2 = new TreeSet();
      for (int i = 0; i < 10; i++) {
         Integer integ = 1000 * i;
         s1.add(integ);
         s2.add(integ);
      }
      marshallAndAssertEquality(s1);
      marshallAndAssertEquality(s2);
   }

   public void testMarshalledValueMarshalling() throws Exception {
      Person p = new Person();
      p.setName("Bob Dylan");
      MarshalledValue mv = new MarshalledValue(p, true, marshaller);
      marshallAndAssertEquality(mv);
   }

   public void testMarshalledValueGetMarshalling() throws Exception {
      Pojo ext = new Pojo();
      MarshalledValue mv = new MarshalledValue(ext, true, marshaller);
      byte[] bytes = marshaller.objectToByteBuffer(mv);
      MarshalledValue rmv = (MarshalledValue) marshaller.objectFromByteBuffer(bytes);
      assert rmv.equals(mv) : "Writen[" + mv + "] and read[" + rmv + "] objects should be the same";
      assert rmv.get() instanceof Pojo;
   }

   public void testSingletonListMarshalling() throws Exception {
      GlobalTransaction gtx = gtf.newGlobalTransaction(new JGroupsAddress(new IpAddress(12345)), false);
      List l = Collections.singletonList(gtx);
      marshallAndAssertEquality(l);
   }

   public void testTransactionLogMarshalling() throws Exception {
      GlobalTransaction gtx = gtf.newGlobalTransaction(new JGroupsAddress(new IpAddress(12345)), false);
      PutKeyValueCommand command = new PutKeyValueCommand("k", "v", false, null, 0, 0, Collections.<Flag>emptySet());
      TransactionLog.LogEntry entry = new TransactionLog.LogEntry(gtx, command);
      byte[] bytes = marshaller.objectToByteBuffer(entry);
      TransactionLog.LogEntry readObj = (TransactionLog.LogEntry) marshaller.objectFromByteBuffer(bytes);
      assert Arrays.equals(readObj.getModifications(), entry.getModifications()) :
            "Writen[" + entry.getModifications() + "] and read[" + readObj.getModifications() + "] objects should be the same";
      assert readObj.getTransaction().equals(entry.getTransaction()) :
            "Writen[" + entry.getModifications() + "] and read[" + readObj.getModifications() + "] objects should be the same";
   }

   public void testImmutableResponseMarshalling() throws Exception {
      marshallAndAssertEquality(RequestIgnoredResponse.INSTANCE);
      marshallAndAssertEquality(UnsuccessfulResponse.INSTANCE);
   }

   public void testExtendedResponseMarshalling() throws Exception {
      SuccessfulResponse sr = new SuccessfulResponse("Blah");
      ExtendedResponse extended = new ExtendedResponse(sr, false);
      byte[] bytes = marshaller.objectToByteBuffer(extended);
      ExtendedResponse readObj = (ExtendedResponse) marshaller.objectFromByteBuffer(bytes);
      assert extended.getResponse().equals(readObj.getResponse()) :
            "Writen[" + extended.getResponse() + "] and read[" + readObj.getResponse() + "] objects should be the same";
      assert extended.isReplayIgnoredRequests() == readObj.isReplayIgnoredRequests() :
            "Writen[" + extended.isReplayIgnoredRequests() + "] and read[" + readObj.isReplayIgnoredRequests() + "] objects should be the same";
   }

   public void testReplicableCommandsMarshalling() throws Exception {
      StateTransferControlCommand c1 = new StateTransferControlCommand(true);
      byte[] bytes = marshaller.objectToByteBuffer(c1);
      StateTransferControlCommand rc1 = (StateTransferControlCommand) marshaller.objectFromByteBuffer(bytes);
      assert rc1.getCommandId() == c1.getCommandId() : "Writen[" + c1.getCommandId() + "] and read[" + rc1.getCommandId() + "] objects should be the same";
      assert Arrays.equals(rc1.getParameters(), c1.getParameters()) : "Writen[" + c1.getParameters() + "] and read[" + rc1.getParameters() + "] objects should be the same";

      String cacheName = EmbeddedCacheManager.DEFAULT_CACHE_NAME;
      ClusteredGetCommand c2 = new ClusteredGetCommand("key", cacheName, Collections.<Flag>emptySet());
      marshallAndAssertEquality(c2);

      // SizeCommand does not have an empty constructor, so doesn't look to be one that is marshallable.

      GetKeyValueCommand c4 = new GetKeyValueCommand("key", null, Collections.<Flag>emptySet());
      bytes = marshaller.objectToByteBuffer(c4);
      GetKeyValueCommand rc4 = (GetKeyValueCommand) marshaller.objectFromByteBuffer(bytes);
      assert rc4.getCommandId() == c4.getCommandId() : "Writen[" + c4.getCommandId() + "] and read[" + rc4.getCommandId() + "] objects should be the same";
      assert Arrays.equals(rc4.getParameters(), c4.getParameters()) : "Writen[" + c4.getParameters() + "] and read[" + rc4.getParameters() + "] objects should be the same";

      PutKeyValueCommand c5 = new PutKeyValueCommand("k", "v", false, null, 0, 0, Collections.<Flag>emptySet());
      marshallAndAssertEquality(c5);

      RemoveCommand c6 = new RemoveCommand("key", null, null, Collections.<Flag>emptySet());
      marshallAndAssertEquality(c6);

      // EvictCommand does not have an empty constructor, so doesn't look to be one that is marshallable.

      InvalidateCommand c7 = new InvalidateCommand(null, null, "key1", "key2");
      bytes = marshaller.objectToByteBuffer(c7);
      InvalidateCommand rc7 = (InvalidateCommand) marshaller.objectFromByteBuffer(bytes);
      assert rc7.getCommandId() == c7.getCommandId() : "Writen[" + c7.getCommandId() + "] and read[" + rc7.getCommandId() + "] objects should be the same";
      assert Arrays.equals(rc7.getParameters(), c7.getParameters()) : "Writen[" + c7.getParameters() + "] and read[" + rc7.getParameters() + "] objects should be the same";

      InvalidateCommand c71 = new InvalidateL1Command(false, null, null, null, null, null, "key1", "key2");
      bytes = marshaller.objectToByteBuffer(c71);
      InvalidateCommand rc71 = (InvalidateCommand) marshaller.objectFromByteBuffer(bytes);
      assert rc71.getCommandId() == c71.getCommandId() : "Writen[" + c71.getCommandId() + "] and read[" + rc71.getCommandId() + "] objects should be the same";
      assert Arrays.equals(rc71.getParameters(), c71.getParameters()) : "Writen[" + c71.getParameters() + "] and read[" + rc71.getParameters() + "] objects should be the same";

      ReplaceCommand c8 = new ReplaceCommand("key", "oldvalue", "newvalue", 0, 0, Collections.EMPTY_SET);
      marshallAndAssertEquality(c8);

      ClearCommand c9 = new ClearCommand();
      bytes = marshaller.objectToByteBuffer(c9);
      ClearCommand rc9 = (ClearCommand) marshaller.objectFromByteBuffer(bytes);
      assert rc9.getCommandId() == c9.getCommandId() : "Writen[" + c9.getCommandId() + "] and read[" + rc9.getCommandId() + "] objects should be the same";
      assert Arrays.equals(rc9.getParameters(), c9.getParameters()) : "Writen[" + c9.getParameters() + "] and read[" + rc9.getParameters() + "] objects should be the same";

      Map m1 = new HashMap();
      for (int i = 0; i < 10; i++) {
         GlobalTransaction gtx = gtf.newGlobalTransaction(new JGroupsAddress(new IpAddress(1000 * i)), false);
         m1.put(1000 * i, gtx);
      }
      PutMapCommand c10 = new PutMapCommand(m1, null, 0, 0, Collections.EMPTY_SET);
      marshallAndAssertEquality(c10);

      Address local = new JGroupsAddress(new IpAddress(12345));
      GlobalTransaction gtx = gtf.newGlobalTransaction(local, false);
      PrepareCommand c11 = new PrepareCommand(cacheName, gtx, true, c5, c6, c8, c10);
      marshallAndAssertEquality(c11);

      CommitCommand c12 = new CommitCommand(cacheName, gtx);
      marshallAndAssertEquality(c12);

      RollbackCommand c13 = new RollbackCommand(cacheName, gtx);
      marshallAndAssertEquality(c13);

      MultipleRpcCommand c99 = new MultipleRpcCommand(Arrays.asList(c2, c5, c6, c8, c10, c12, c13), cacheName);
      marshallAndAssertEquality(c99);
   }

   public void testRehashControlCommand() throws Exception {
      Cache<Object,Object> cache = cm.getCache();

      String cacheName = EmbeddedCacheManager.DEFAULT_CACHE_NAME;
      ImmortalCacheEntry entry1 = (ImmortalCacheEntry) InternalEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, -1, System.currentTimeMillis(), -1);
      Map<Object, InternalCacheValue> state = new HashMap<Object, InternalCacheValue>();
      state.put(new MagicKey(cache, "magic_key"), entry1.toInternalCacheValue());
      Address a1 = new JGroupsAddress(UUID.randomUUID());
      Address a2 = new JGroupsAddress(UUID.randomUUID());
      Address a3 = new JGroupsAddress(UUID.randomUUID());
      Set<Address> oldAddresses = new LinkedHashSet();
      oldAddresses.add(a1);
      oldAddresses.add(a2);
      DefaultConsistentHash oldCh = new DefaultConsistentHash(new MurmurHash3());
      oldCh.setCaches(oldAddresses);
      Set<Address> newAddresses = new LinkedHashSet();
      newAddresses.add(a1);
      newAddresses.add(a2);
      newAddresses.add(a3);
      DefaultConsistentHash newCh = new DefaultConsistentHash(new MurmurHash2());
      newCh.setCaches(newAddresses);
      RehashControlCommand c14 = new RehashControlCommand(cacheName, RehashControlCommand.Type.APPLY_STATE, a1, 99, state, oldCh, newCh);
      byte[] bytes = marshaller.objectToByteBuffer(c14);
      marshaller.objectFromByteBuffer(bytes);

      bytes = marshaller.objectToByteBuffer(c14);
      marshaller.objectFromByteBuffer(bytes);

      bytes = marshaller.objectToByteBuffer(c14);
      marshaller.objectFromByteBuffer(bytes);
   }

   public void test000() throws ClassNotFoundException, IOException {
      byte[] bytes = {3, 1, -2, 3, 74, 0, 0, 17, 0, 0, 0, 4, 100, 105, 115, 116, -52, 2, 3, 1, -2, 6, 73, 0, 3, 39, 4, 10, 0, 0, 0, 21, 111, 114, 103, 46, 106, 103, 114, 111, 117, 112, 115, 46, 117, 116, 105, 108, 46, 85, 85, 73, 68, 55, 34, -52, 74, 28, -68, 13, 92, 50, 16, -56, -4, -32, 96, 91, -106, -114, -128, 26, 71, -50, 106, -52, -69, -36, -109, 53, 75, 0, 0, 0, 2, 3, 2, 0, 1, 4, 9, 0, 0, 0, 36, 111, 114, 103, 46, 105, 110, 102, 105, 110, 105, 115, 112, 97, 110, 46, 100, 105, 115, 116, 114, 105, 98, 117, 116, 105, 111, 110, 46, 77, 97, 103, 105, 99, 75, 101, 121, -12, 104, -127, 32, 29, 91, -126, -98, 0, 0, 0, 3, 0, 0, 0, 7, 97, 100, 100, 114, 101, 115, 115, 20, 0, 0, 0, 0, 8, 104, 97, 115, 104, 99, 111, 100, 101, 35, 0, 0, 0, 0, 4, 110, 97, 109, 101, 20, 0, 22, 62, 11, 78, 111, 100, 101, 65, 45, 51, 50, 54, 50, 56, 122, -66, -70, -47, 62, 2, 107, 50, 3, 14, 62, 2, 118, 50, 3, 51, 0, 0, 0, 1, 3, 73, 83, 2, 95, 3, 39, 4, 59, -2, 50, 16, -56, -4, -32, 96, 91, -106, -114, -128, 26, 71, -50, 106, -52, -69, -36, -109, 53, 3, 39, 4, 59, -2, 50, 16, 73, -73, 104, 36, -62, 59, 74, 125, 126, 57, -12, 55, 10, -121, -44, -82, 53, 3, 51, 0, 0, 0, 1, 3, 73, 83, 3, 95, 3, 39, 4, 59, -2, 50, 16, -56, -4, -32, 96, 91, -106, -114, -128, 26, 71, -50, 106, -52, -69, -36, -109, 53, 3, 39, 4, 59, -2, 50, 16, 73, -73, 104, 36, -62, 59, 74, 125, 126, 57, -12, 55, 10, -121, -44, -82, 53, 3, 39, 4, 59, -2, 50, 16, 19, 93, 117, 78, -28, -19, -41, 13, 38, 93, -96, -106, -106, -6, 0, 68, 53};
      marshaller.objectFromByteBuffer(bytes);
   }

   public void testMultiRpcCommand() throws Exception {
      String cacheName = EmbeddedCacheManager.DEFAULT_CACHE_NAME;
      ClusteredGetCommand c2 = new ClusteredGetCommand("key", cacheName, Collections.<Flag>emptySet());
      PutKeyValueCommand c5 = new PutKeyValueCommand("k", "v", false, null, 0, 0, Collections.<Flag>emptySet());
      MultipleRpcCommand c99 = new MultipleRpcCommand(Arrays.<ReplicableCommand>asList(c2, c5), cacheName);
      marshallAndAssertEquality(c99);
   }

   public void testInternalCacheEntryMarshalling() throws Exception {
      ImmortalCacheEntry entry1 = (ImmortalCacheEntry) InternalEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, -1, System.currentTimeMillis(), -1);
      marshallAndAssertEquality(entry1);

      MortalCacheEntry entry2 = (MortalCacheEntry) InternalEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, 200000, System.currentTimeMillis(), -1);
      marshallAndAssertEquality(entry2);

      TransientCacheEntry entry3 = (TransientCacheEntry) InternalEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, -1, System.currentTimeMillis(), 4000000);
      marshallAndAssertEquality(entry3);

      TransientMortalCacheEntry entry4 = (TransientMortalCacheEntry) InternalEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, 200000, System.currentTimeMillis(), 4000000);
      marshallAndAssertEquality(entry4);
   }

   public void testInternalCacheValueMarshalling() throws Exception {
      ImmortalCacheValue value1 = (ImmortalCacheValue) InternalEntryFactory.createValue("value", System.currentTimeMillis() - 1000, -1, System.currentTimeMillis(), -1);
      byte[] bytes = marshaller.objectToByteBuffer(value1);
      ImmortalCacheValue rvalue1 = (ImmortalCacheValue) marshaller.objectFromByteBuffer(bytes);
      assert rvalue1.getValue().equals(value1.getValue()) : "Writen[" + rvalue1.getValue() + "] and read[" + value1.getValue() + "] objects should be the same";

      MortalCacheValue value2 = (MortalCacheValue) InternalEntryFactory.createValue("value", System.currentTimeMillis() - 1000, 200000, System.currentTimeMillis(), -1);
      bytes = marshaller.objectToByteBuffer(value2);
      MortalCacheValue rvalue2 = (MortalCacheValue) marshaller.objectFromByteBuffer(bytes);
      assert rvalue2.getValue().equals(value2.getValue()) : "Writen[" + rvalue2.getValue() + "] and read[" + value2.getValue() + "] objects should be the same";

      TransientCacheValue value3 = (TransientCacheValue) InternalEntryFactory.createValue("value", System.currentTimeMillis() - 1000, -1, System.currentTimeMillis(), 4000000);
      bytes = marshaller.objectToByteBuffer(value3);
      TransientCacheValue rvalue3 = (TransientCacheValue) marshaller.objectFromByteBuffer(bytes);
      assert rvalue3.getValue().equals(value3.getValue()) : "Writen[" + rvalue3.getValue() + "] and read[" + value3.getValue() + "] objects should be the same";

      TransientMortalCacheValue value4 = (TransientMortalCacheValue) InternalEntryFactory.createValue("value", System.currentTimeMillis() - 1000, 200000, System.currentTimeMillis(), 4000000);
      bytes = marshaller.objectToByteBuffer(value4);
      TransientMortalCacheValue rvalue4 = (TransientMortalCacheValue) marshaller.objectFromByteBuffer(bytes);
      assert rvalue4.getValue().equals(value4.getValue()) : "Writen[" + rvalue4.getValue() + "] and read[" + value4.getValue() + "] objects should be the same";
   }

   public void testBucketMarshalling() throws Exception {
      ImmortalCacheEntry entry1 = (ImmortalCacheEntry) InternalEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, -1, System.currentTimeMillis(), -1);
      MortalCacheEntry entry2 = (MortalCacheEntry) InternalEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, 200000, System.currentTimeMillis(), -1);
      TransientCacheEntry entry3 = (TransientCacheEntry) InternalEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, -1, System.currentTimeMillis(), 4000000);
      TransientMortalCacheEntry entry4 = (TransientMortalCacheEntry) InternalEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, 200000, System.currentTimeMillis(), 4000000);
      Bucket b = new Bucket();
      b.setBucketId(0);
      b.addEntry(entry1);
      b.addEntry(entry2);
      b.addEntry(entry3);
      b.addEntry(entry4);

      byte[] bytes = marshaller.objectToByteBuffer(b);
      Bucket rb = (Bucket) marshaller.objectFromByteBuffer(bytes);
      assert rb.getEntries().equals(b.getEntries()) : "Writen[" + b.getEntries() + "] and read[" + rb.getEntries() + "] objects should be the same";
   }

   public void testLongPutKeyValueCommand() throws Exception {
      PutKeyValueCommand c = new PutKeyValueCommand("SESSION_173", "@TSXMHVROYNOFCJVEUJQGBCENNQDEWSCYSOHECJOHEICBEIGJVTIBB@TVNCWLTQCGTEJ@NBJLTMVGXCHXTSVE@BCRYGWPRVLXOJXBRJDVNBVXPRTRLBMHPOUYQKDEPDSADUAWPFSIOCINPSSFGABDUXRMTMMJMRTGBGBOAMGVMTKUDUAJGCAHCYW@LAXMDSFYOSXJXLUAJGQKPTHUKDOXRWKEFIVRTH@VIMQBGYPKWMS@HPOESTPIJE@OTOTWUWIOBLYKQQPTNGWVLRRCWHNIMWDQNOO@JHHEVYVQEODMWKFKKKSWURVDLXPTFQYIHLIM@GSBFWMDQGDQIJONNEVHGQTLDBRBML@BEWGHOQHHEBRFUQSLB@@CILXEAVQQBTXSITMBXHMHORHLTJF@MKMHQGHTSENWILTAKCCPVSQIPBVRAFSSEXIOVCPDXHUBIBUPBSCGPRECXEPMQHRHDOHIHVBPNDKOVLPCLKAJMNOTSF@SRXYVUEMQRCXVIETXVHOVNGYERBNM@RIMGHC@FNTUXSJSKALGHAFHGTFEANQUMBPUYFDSGLUYRRFDJHCW@JBWOBGMGTITAICRC@TPVCRKRMFPUSRRAHI@XOYKVGPHEBQD@@APEKSBCTBKREWAQGKHTJ@IHJD@YFSRDQPA@HKKELIJGFDYFEXFCOTCQIHKCQBLVDFHMGOWIDOWMVBDSJQOFGOIAPURRHVBGEJWYBUGGVHE@PU@NMQFMYTNYJDWPIADNVNCNYCCCPGODLAO@YYLVITEMNNKIFSDXKORJYWMFGKNYFPUQIC@AIDR@IWXCVALQBDOXRWIBXLKYTWDNHHSCUROAU@HVNENDAOP@RPTRIGLLLUNDQIDXJDDNF@P@PA@FEIBQKSKFQITTHDYGQRJMWPRLQC@NJVNVSKGOGYXPYSQHKPALKLFWNAOSQFTLEPVOII@RPDNRCVRDUMMFIVSWGIASUBMTGQSDGB@TBBYECFBRBGILJFCJ@JIQIQRVJXWIPGNVXKYATSPJTIPGCMCNPOKNEHBNUIAEQFQTYVLGAR@RVWVA@RMPBX@LRLJUEBUWO@PKXNIP@FKIQSVWKNO@FOJWDSIOLXHXJFBQPPVKKP@YKXPOOMBTLXMEHPRLLSFSVGMPXXNBCYVVSPNGMFBJUDCVOVGXPKVNTOFKVJUJOSDHSCOQRXOKBVP@WCUUFGMJAUQ@GRAGXICFCFICBSNASUBPAFRIPUK@OXOCCNOGTTSFVQKBQNB@DWGVEFSGTAXAPLBJ@SYHUNXWXPMR@KPFAJCIXPDURELFYPMUSLTJSQNDHHKJTIWCGNEKJF@CUWYTWLPNHYPHXNOGLSICKEFDULIXXSIGFMCQGURSRTUJDKRXBUUXIDFECMPXQX@CVYLDABEMFKUGBTBNMNBPCKCHWRJKSOGJFXMFYLLPUVUHBCNULEFAXPVKVKQKYCEFRUYPBRBDBDOVYLIQMQBLTUK@PRDCYBOKJGVUADFJFAFFXKJTNAJTHISWOSMVAYLIOGIORQQWFAKNU@KHPM@BYKTFSLSRHBATQTKUWSFAQS@Y@QIKCUWQYTODBRCYYYIAFMDVRURKVYJXHNGVLSQQFCXKLNUPCTEJSWIJUBFELSBUHANELHSIWLVQSSAIJRUEDOHHX@CKEBPOJRLRHEPLENSCDGEWXRTVUCSPFSAJUXDJOIUWFGPKHBVRVDMUUCPUDKRKVAXPSOBOPKPRRLFCKTLH@VGWKERASJYU@JAVWNBJGQOVF@QPSGJVEPAV@NAD@@FQRYPQIOAURILWXCKINPMBNUHPUID@YDQBHWAVDPPWRFKKGWJQTI@@OPSQ@ROUGHFNHCJBDFCHRLRTEMTUBWVCNOPYXKSSQDCXTOLOIIOCXBTPAUYDICFIXPJRB@CHFNXUCXANXYKXAISDSSLJGQOLBYXWHG@@KPARPCKOXAYVPDGRW@LDCRQBNMJREHWDYMXHEXAJQKHBIRAVHJQIVGOIXNINYQMJBXKM@DXESMBHLKHVSFDLVPOSOVMLHPSHQYY@DNMCGGGAJMHPVDLBGJP@EVDGLYBMD@NWHEYTBPIBPUPYOPOJVV@IVJXJMHIWWSIRKUWSR@U@@TDVMG@GRXVLCNEIISEVIVPOMJHKOWMRMITYDUQASWJIKVNYUFQVDT@BHTOMFXVFRKAARLNOGX@ADWCKHOVEMIGBWXINCUXEMVHSJJQDU@INTHDJQPSAQNAYONDBBFYGBTNGUSJHRKLCPHQMNLDHUQJPLLCDVTYLXTHJCBUXCRDY@YI@IQDCLJBBJC@NXGANXFIWPPNFVTDJWQ@@BIYJONOFP@RHTQEYPVHPPUS@UUENSNNF@WVGTSAVKDSQNMHP@VJORGTVWXVBPWKQNRWLSQFSBMXQKWRYMXPAYREXYGONKEWJMBCSLB@KSHXMIWMSBDGQWPDMUGVNMEWKMJKQECIRRVXBPBLGAFTUFHYSHLF@TGYETMDXRFAXVEUBSTGLSMWJMXJWMDPPDAFGNBMTQEMBDLRASMUMU@QTCDCPEGODHESDQVEIQYBJJPFXDLWPUNFAREYCY@YDDSTMKWCANNPXF@@WLMEXRPUNTWNOX@YKFNNTGMXIBBDA@TYLPJFNFHPQKMSNCLBME@FBPOIYNSDFBLHITKIFEFNXXOJAAFMRTGPALOANXF@YPY@RYTVOW@AKNM@C@LJKGBJMUYGGTXRHQCPOLNOGPPS@YSKAJSTQHLRBXUACXJYBLJSEHDNMLLUBSOIHQUI@VUNF@XAVRXUCYNCBDDGUDNVRYP@TPFPKGVNPTEDOTTUUFKCHQ@WWASQXLCBHNRBVSD@NVYT@GJQYSQGYPJO@WSEYDVKCBWANAFUWLDXOQYCYP@BSJFCBTXGKUNWLWUCYL@TNOWGDFHQTWQVYLQBBRQVMGNDBVXEFXTMMVYSHNVTTQAJCHKULOAJUSGJRPHQFCROWE@OMFUVRKGCWED@IAQGRLADOJGQKLCL@FCKTSITGMJRCCMPLOS@ONPQWFUROXYAUJQXIYVDCYBPYHPYCXNCRKRKLATLWWXLBLNOPUJFUJEDOIRKS@MMYPXIJNXPFOQJCHSCBEBGDUQYXQAWEEJDOSINXYLDXUJCQECU@WQSACTDFLGELHPGDFVDXFSSFOSYDLHQFVJESNAVAHKTUPBTPLSFSHYKLEXJXGWESVQQUTUPU@QXRTIDQ@IXBBOYINNHPEMTPRVRNJPQJFACFXUBKXOFHQSPOTLCQ@PLWGEFNKYCYFMKWPFUP@GLHKNMASGIENCACUISTG@YNQCNSOSBKOIXORKSHEOXHSMJJRUICJTCK@PWFRBPLXU@MUEMPFGDLUJEKD@ROUFBLKATXUCHEAQHEYDLCFDIRJSAXTV@CYMPQNMLTMFAHPRBLNSCVFBJMKQLAHWYIOLRMTOY@@RNKTUXHFYUMHGKCCGNEOIOQCISJEHCEVTTWM@TLFRIFDREHFBTTDEJRUNTWAEETGSVDOR@@UQNKFERMBVFJBOAYHPOKMSMRIERDA@JXYSJ@ORER@MBAVWCVGFNA@FRRPQSIIOIUGAJKVQXGINUUKPJPLQRMHPUBETEEIMIBPM@PETR@XD@DOHGRIBVXKLXQWHUFMTWEDYWFWRLPGDS@TANUXGIDTRVXKVCVEXYRKXQCTI@WNSFRAHJJGG@NIPPAAOJXQRTCLBYKDA@FFGHNUIGBFKOQMEDUEFELFLNKPCHA@OXJJRYNPDFSXIFSJYTDMSSBHDPUSQQDAVD@JAAWJDSVTERAJBFEPVRWKMYAPISPWLDPSRE@UMRQLXERTWRDLQVMVCOM@NYPXFLWMWKALMQVNJ@HCTMMIOLRWBJHCYFLMM@IWXPSHRRUNICSSWHOQHUVJE@HKJAADLBTPVLDAKCHRSURJCAXYTMYKHQMWDAWWASUW@HWGBVPTRHJGDWOGHPCNWSXTNKWONQGEKDDWGCKWVSAD@YLCCENMCHALHVDYQW@NQGNCY@M@GGV@RIR@OUS@PQIJMCFEIMGPYBXYR@NSIAUEXT@MOCNWRMLYHUUAFJCCLLRNFGKLPPIIH@BYRME@UJAKIFHOV@ILP@BGXRNJBIBARSOIMTDSHMGPIGRJBGHYRYXPFUHVOOMCQFNLM@CNCBTGO@UKXBOICNVCRGHADYQVAMNSFRONJ@WITET@BSHMQLWYMVGMQJVSJOXOUJDSXYVVBQJSVGREQLIQKWC@BMDNONHXFYPQENSJINQYKHVCTUTG@QQYJKJURDCKJTUQAM@DWNXWRNILYVAAJ@IADBIXKEIHVXLXUVMGQPAQTWJCDMVDVYUDTXQTCYXDPHKBAGMTAMKEM@QNOQJBREXNWFCXNXRPGOGEIR@KQJIGXAWXLTNCX@ID@XNRNYGRF@QPNWEX@XH@XKSXLQTLQPFSHAHXJLHUTNQWFFAJYHBWIFVJELDPSPLRRDPPNXSBYBEREEELIWNVYXOXYJQAIGHALUAWNUSSNMBHBFLRMMTKEKNSINECUGWTDNMROXI@BJJXKSPIIIXOAJBFVSITQDXTODBGKEPJMWK@JOL@SWTCGSHCOPHECTPJFUXIHUOSVMUTNNSLLJDEOMAGIXEAAVILRMOJXVHHPNPUYYODMXYAYGHI@BUB@NLP@KNPCYFRWAFES@WISBACDSPELEVTJEBNRVENSXXEVDVC@RIDIDSBPQIQNNSRPS@HCJ@XPIOFDXHUBCNFQKHMUYLXW@LMFMALHLESSXCOULRWDTJIVKKTLGFE@HKGVKUGMVHWACQOTSVNWBNUUGTMSQEJ@DXJQQYPOWVRQNQKXSLOEAA@@FRDCGCCQWQ@IY@EATGQGQIETPIJHOIQRYWLTGUENQYDNQSBI@IAUDEWDKICHNUGNAIXNICMBK@CJGSASMTFKWOBSI@KULNENWXV@VNFOANM@OJHFVV@IYRMDB@LHSGXIJMMFCGJKTKDXSMY@FHDNY@VSDUORGWVFMVKJXOCCDLSLMHCSXFBTW@RQTFNRDJUIKRD@PWPY", false, null, 0, 0, Collections.<Flag>emptySet());
      marshallAndAssertEquality(c);
   }

   public void testExceptionResponse() throws Exception {
      ExceptionResponse er = new ExceptionResponse(new TimeoutException());
      byte[] bytes = marshaller.objectToByteBuffer(er);
      ExceptionResponse rer = (ExceptionResponse) marshaller.objectFromByteBuffer(bytes);
      assert rer.getException().getClass().equals(er.getException().getClass()) : "Writen[" + er.getException().getClass() + "] and read[" + rer.getException().getClass() + "] objects should be the same";
   }

   public void testAtomicHashMap() throws Exception {
      AtomicHashMap<String, String> m = new AtomicHashMap<String, String>();
      m.initForWriting();
      m.put("k1", "v1");
      m.put("k1", "v2");
      m.put("k1", "v3");
      assert m.size() == 1;
      byte[] bytes = marshaller.objectToByteBuffer(m);
      m = (AtomicHashMap<String, String>) marshaller.objectFromByteBuffer(bytes);
      for (Map.Entry<String, String> entry : m.entrySet()) {
         assert m.get(entry.getKey()).equals(entry.getValue());
      }
      assert m.size() == 1;

      m = new AtomicHashMap<String, String>();
      assert m.isEmpty();
      bytes = marshaller.objectToByteBuffer(m);
      m = (AtomicHashMap<String, String>) marshaller.objectFromByteBuffer(bytes);
      assert m.isEmpty();

      m = new AtomicHashMap<String, String>();
      m.initForWriting();
      m.put("k1", "v1");
      m.put("k2", "v2");
      m.put("k3", "v3");
      m.remove("k1");
      assert m.size() == 2;
      bytes = marshaller.objectToByteBuffer(m);
      m = (AtomicHashMap<String, String>) marshaller.objectFromByteBuffer(bytes);
      for (Map.Entry<String, String> entry : m.entrySet()) {
         assert m.get(entry.getKey()).equals(entry.getValue());
      }
      assert m.size() == 2;

      m = new AtomicHashMap<String, String>();
      m.initForWriting();
      m.put("k5", "v1");
      m.put("k5", "v2");
      m.put("k5", "v3");
      m.clear();
      assert m.isEmpty();
      bytes = marshaller.objectToByteBuffer(m);
      m = (AtomicHashMap<String, String>) marshaller.objectFromByteBuffer(bytes);
      for (Map.Entry<String, String> entry : m.entrySet()) {
         assert m.get(entry.getKey()).equals(entry.getValue());
      }
      assert m.isEmpty();
   }

   public void testMarshallObjectThatContainsACustomReadObjectMethod() throws Exception {
      ObjectThatContainsACustomReadObjectMethod obj = new ObjectThatContainsACustomReadObjectMethod();
      obj.anObjectWithCustomReadObjectMethod = new CustomReadObjectMethod();
      marshallAndAssertEquality(obj);
   }

   public void testMIMECacheEntryMarshalling() throws Exception {
      MIMECacheEntry entry = new MIMECacheEntry("rm", new byte[] {1, 2, 3});
      byte[] bytes = marshaller.objectToByteBuffer(entry);
      MIMECacheEntry rEntry = (MIMECacheEntry) marshaller.objectFromByteBuffer(bytes);
      assert Arrays.equals(rEntry.data, entry.data);
      assert rEntry.contentType.equals(entry.contentType);
      assert rEntry.lastModified == entry.lastModified;
   }

   public void testNestedNonSerializable() throws Exception {
      PutKeyValueCommand cmd = new PutKeyValueCommand("k", new Object(), false, null, 0, 0, Collections.<Flag>emptySet());
      try {
         marshaller.objectToByteBuffer(cmd);
      } catch (NotSerializableException e) {
         log.info("Log exception for output format verification", e);
         TraceInformation inf = (TraceInformation) e.getCause();
         assert inf.toString().contains("in object java.lang.Object@");
         assert inf.toString().contains("in object org.infinispan.commands.write.PutKeyValueCommand@");
      }
   }

   public void testNonSerializable() throws Exception {
      try {
         marshaller.objectToByteBuffer(new Object());
      } catch (NotSerializableException e) {
         log.info("Log exception for output format verification", e);
         TraceInformation inf = (TraceInformation) e.getCause();
         assert inf.toString().contains("in object java.lang.Object@");
      }
   }

   public void testByteArrayKey() throws Exception {
      ByteArrayKey o = new ByteArrayKey("123".getBytes());
      marshallAndAssertEquality(o);
   }

   public void testConcurrentHashMap() throws Exception {
      ConcurrentHashMap map = new ConcurrentHashMap();
      map.put(1, "v1");
      map.put(2, "v2");
      map.put(3, "v3");
      marshallAndAssertEquality(map);
   }

   public static class PojoWhichFailsOnUnmarshalling extends Pojo {
      private static final long serialVersionUID = -5109779096242560884L;

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         throw new IOException("Injected failue!");
      }

   }

   public void testErrorUnmarshalling() throws Exception {
      Pojo pojo = new PojoWhichFailsOnUnmarshalling();
      byte[] bytes = marshaller.objectToByteBuffer(pojo);
      try {
         marshaller.objectFromByteBuffer(bytes);
      } catch (IOException e) {
         log.info("Log exception for output format verification", e);
         TraceInformation inf = (TraceInformation) e.getCause();
         assert inf.toString().contains("in object of type org.infinispan.marshall.VersionAwareMarshallerTest$PojoWhichFailsOnUnmarshalling");
      }

   }

   public void testMarshallingSerializableSubclass() throws Exception {
      Child1 child1Obj = new Child1(1234, "1234");
      byte[] bytes = marshaller.objectToByteBuffer(child1Obj);
      marshaller.objectFromByteBuffer(bytes);
   }

   public void testMarshallingNestedSerializableSubclass() throws Exception {
      Child1 child1Obj = new Child1(1234, "1234");
      Child2 child2Obj = new Child2(2345, "2345", child1Obj);
      byte[] bytes = marshaller.objectToByteBuffer(child2Obj);
      marshaller.objectFromByteBuffer(bytes);
   }

   public void testPojoWithJBossMarshallingExternalizer(Method m) throws Exception {
      PojoWithJBossExternalize pojo = new PojoWithJBossExternalize(27, k(m));
      marshallAndAssertEquality(pojo);
   }

   protected void marshallAndAssertEquality(Object writeObj) throws Exception {
      byte[] bytes = marshaller.objectToByteBuffer(writeObj);
      Object readObj = marshaller.objectFromByteBuffer(bytes);
      assert readObj.equals(writeObj) : "Writen[" + writeObj + "] and read[" + readObj + "] objects should be the same";
   }

   public static class Pojo implements Externalizable {
      int i;
      boolean b;
      static int serializationCount, deserializationCount;
      private static final long serialVersionUID = 9032309454840083326L;

      public boolean equals(Object o) {
         if (this == o) {
            return true;
         }
         if (o == null || getClass() != o.getClass()) {
            return false;
         }

         Pojo pojo = (Pojo) o;

         if (b != pojo.b) {
            return false;
         }
         if (i != pojo.i) {
            return false;
         }

         return true;
      }

      public int hashCode() {
         int result;
         result = i;
         result = 31 * result + (b ? 1 : 0);
         return result;
      }

      public void writeExternal(ObjectOutput out) throws IOException {
         out.writeInt(i);
         out.writeBoolean(b);
         serializationCount++;
      }

      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         i = in.readInt();
         b = in.readBoolean();
         deserializationCount++;
      }
   }

   static class Parent implements Serializable {
       private final String id;
       private final Child1 child1Obj;

       public Parent(String id, Child1 child1Obj) {
           this.id = id;
           this.child1Obj = child1Obj;
       }

       public String getId() {
           return id;
       }
       public Child1 getChild1Obj() {
           return child1Obj;
       }
   }

   static class Child1 extends Parent {
       private final int someInt;

       public Child1(int someInt, String parentStr) {
           super(parentStr, null);
           this.someInt = someInt;
       }

   }

   static class Child2 extends Parent {
       private final int someInt;

       public Child2(int someInt, String parentStr, Child1 child1Obj) {
           super(parentStr, child1Obj);
           this.someInt = someInt;
       }
   }

}
