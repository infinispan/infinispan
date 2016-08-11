package org.infinispan.marshall;

import static org.infinispan.test.TestingUtil.extractGlobalMarshaller;
import static org.infinispan.test.TestingUtil.k;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.io.ByteArrayInputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.Cache;
import org.infinispan.atomic.impl.AtomicHashMap;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderCommitCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderNonVersionedPrepareCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderRollbackCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderVersionedCommitCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderVersionedPrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.marshall.NotSerializableException;
import org.infinispan.commons.marshall.PojoWithJBossExternalize;
import org.infinispan.commons.marshall.PojoWithSerializeWith;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.FastCopyHashMap;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheValue;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.Flag;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.JBossMarshallingTest.CustomReadObjectMethod;
import org.infinispan.marshall.core.JBossMarshallingTest.ObjectThatContainsACustomReadObjectMethod;
import org.infinispan.marshall.core.MarshalledValue;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.remoting.MIMECacheEntry;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.marshalling.TraceInformation;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.UUID;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "marshall.VersionAwareMarshallerTest")
public class VersionAwareMarshallerTest extends AbstractInfinispanTest {

   private static final Log log = LogFactory.getLog(VersionAwareMarshallerTest.class);
   private StreamingMarshaller marshaller;
   private EmbeddedCacheManager cm;

   private final TransactionFactory gtf = new TransactionFactory();

   public VersionAwareMarshallerTest() {
      gtf.init(false, false, true, false);
   }

   @BeforeClass
   public void setUp() {
      // Use a clustered cache manager to be able to test global marshaller interaction too
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      cm = TestCacheManagerFactory.createClusteredCacheManager(builder);
      marshaller = extractGlobalMarshaller(cm);
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
      List<GlobalTransaction> l1 = new ArrayList<GlobalTransaction>();
      List<GlobalTransaction> l2 = new LinkedList<GlobalTransaction>();
      for (int i = 0; i < 10; i++) {
         JGroupsAddress jGroupsAddress = new JGroupsAddress(new IpAddress("localhost", 1000 * i));
         GlobalTransaction gtx = gtf.newGlobalTransaction(jGroupsAddress, false);
         l1.add(gtx);
         l2.add(gtx);
      }
      marshallAndAssertEquality(l1);
      marshallAndAssertEquality(l2);
   }

   public void testMapMarshalling() throws Exception {
      Map<Integer, GlobalTransaction> m1 = new HashMap<Integer, GlobalTransaction>();
      Map<Integer, GlobalTransaction> m2 = new TreeMap<Integer, GlobalTransaction>();
      Map<Integer, GlobalTransaction> m3 = new HashMap<Integer, GlobalTransaction>();
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
      Set<Integer> s1 = new HashSet<Integer>();
      Set<Integer> s2 = new TreeSet<Integer>();
      for (int i = 0; i < 10; i++) {
         Integer integ = 1000 * i;
         s1.add(integ);
         s2.add(integ);
      }
      marshallAndAssertEquality(s1);
      marshallAndAssertEquality(s2);
   }

   public void testTreeSetWithComparator() throws Exception {
      Set<Human> treeSet = new TreeSet<Human>(new HumanComparator());
      for (int i = 0; i < 10; i++) {
         treeSet.add(new Human().age(i));
      }
      marshallAndAssertEquality(treeSet);
   }

   public void testMarshalledValueMarshalling() throws Exception {
      Person p = new Person();
      p.setName("Bob Dylan");
      MarshalledValue mv = new MarshalledValue(p, marshaller);
      marshallAndAssertEquality(mv);
   }

   public void testMarshalledValueGetMarshalling() throws Exception {
      Pojo ext = new Pojo();
      MarshalledValue mv = new MarshalledValue(ext, marshaller);
      byte[] bytes = marshaller.objectToByteBuffer(mv);
      MarshalledValue rmv = (MarshalledValue) marshaller.objectFromByteBuffer(bytes);
      assert rmv.equals(mv) : "Writen[" + mv + "] and read[" + rmv + "] objects should be the same";
      assert rmv.get() instanceof Pojo;
   }

   public void testSingletonListMarshalling() throws Exception {
      GlobalTransaction gtx = gtf.newGlobalTransaction(new JGroupsAddress(new IpAddress(12345)), false);
      List<GlobalTransaction> l = Collections.singletonList(gtx);
      marshallAndAssertEquality(l);
   }

   public void testImmutableResponseMarshalling() throws Exception {
      marshallAndAssertEquality(UnsuccessfulResponse.INSTANCE);
   }

   public void testReplicableCommandsMarshalling() throws Exception {
      ByteString cacheName = ByteString.fromString(EmbeddedCacheManager.DEFAULT_CACHE_NAME);
      ClusteredGetCommand c2 = new ClusteredGetCommand("key", cacheName,
                                                       EnumUtil.EMPTY_BIT_SET, AnyEquivalence.getInstance());
      marshallAndAssertEquality(c2);

      // SizeCommand does not have an empty constructor, so doesn't look to be one that is marshallable.

      GetKeyValueCommand c4 = new GetKeyValueCommand("key", EnumUtil.EMPTY_BIT_SET);
      marshallAndAssertEquality(c4);

      PutKeyValueCommand c5 = new PutKeyValueCommand("k", "v", false, null,
            new EmbeddedMetadata.Builder().build(), EnumUtil.EMPTY_BIT_SET, AnyEquivalence.getInstance(), CommandInvocationId.generateId(null));
      marshallAndAssertEquality(c5);

      RemoveCommand c6 = new RemoveCommand("key", null, null, EnumUtil.EMPTY_BIT_SET, AnyEquivalence.getInstance(), CommandInvocationId.generateId(null));
      marshallAndAssertEquality(c6);

      // EvictCommand does not have an empty constructor, so doesn't look to be one that is marshallable.

      InvalidateCommand c7 = new InvalidateCommand(null, EnumUtil.EMPTY_BIT_SET, CommandInvocationId.generateId(null), "key1", "key2");
      marshallAndAssertEquality(c7);

      InvalidateCommand c71 = new InvalidateL1Command(null, null, null, EnumUtil.EMPTY_BIT_SET, CommandInvocationId.generateId(null), "key1", "key2");
      marshallAndAssertEquality(c71);

      ReplaceCommand c8 = new ReplaceCommand("key", "oldvalue", "newvalue",
            null, new EmbeddedMetadata.Builder().build(), EnumUtil.EMPTY_BIT_SET, AnyEquivalence.getInstance(), CommandInvocationId.generateId(null));
      marshallAndAssertEquality(c8);

      ClearCommand c9 = new ClearCommand();
      marshallAndAssertEquality(c9);

      Map<Integer, GlobalTransaction> m1 = new HashMap<Integer, GlobalTransaction>();
      for (int i = 0; i < 10; i++) {
         GlobalTransaction gtx = gtf.newGlobalTransaction(new JGroupsAddress(new IpAddress(1000 * i)), false);
         m1.put(1000 * i, gtx);
      }
      PutMapCommand c10 = new PutMapCommand(m1, null, new EmbeddedMetadata.Builder().build(), EnumUtil.EMPTY_BIT_SET, CommandInvocationId.generateId(null));
      marshallAndAssertEquality(c10);

      Address local = new JGroupsAddress(new IpAddress(12345));
      GlobalTransaction gtx = gtf.newGlobalTransaction(local, false);
      PrepareCommand c11 = new PrepareCommand(cacheName, gtx, true, c5, c6, c8, c10);
      marshallAndAssertEquality(c11);

      CommitCommand c12 = new CommitCommand(cacheName, gtx);
      marshallAndAssertEquality(c12);

      RollbackCommand c13 = new RollbackCommand(cacheName, gtx);
      marshallAndAssertEquality(c13);

      TotalOrderNonVersionedPrepareCommand c14 = new TotalOrderNonVersionedPrepareCommand(cacheName, gtx, c5, c6, c8, c10);
      marshallAndAssertEquality(c14);

      TotalOrderVersionedPrepareCommand c15 = new TotalOrderVersionedPrepareCommand(cacheName, gtx, Arrays.<WriteCommand>asList(c5, c10), true);
      c15.setVersionsSeen(new EntryVersionsMap());
      marshallAndAssertEquality(c15);

      TotalOrderRollbackCommand c16 = new TotalOrderRollbackCommand(cacheName, gtx);
      marshallAndAssertEquality(c16);

      TotalOrderCommitCommand c17 = new TotalOrderCommitCommand(cacheName, gtx);
      marshallAndAssertEquality(c17);

      TotalOrderVersionedCommitCommand c18 = new TotalOrderVersionedCommitCommand(cacheName, gtx);
      marshallAndAssertEquality(c18);
   }

   public void testStateTransferControlCommand() throws Exception {
      Cache<Object,Object> cache = cm.getCache();

      ByteString cacheName = ByteString.fromString(EmbeddedCacheManager.DEFAULT_CACHE_NAME);
      ImmortalCacheEntry entry1 = (ImmortalCacheEntry) TestInternalCacheEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, -1, System.currentTimeMillis(), -1);
      Collection<InternalCacheEntry> state = new ArrayList<InternalCacheEntry>();
      state.add(entry1);
      Address a1 = new JGroupsAddress(UUID.randomUUID());
      Address a2 = new JGroupsAddress(UUID.randomUUID());
      Address a3 = new JGroupsAddress(UUID.randomUUID());
      List<Address> oldAddresses = new ArrayList<Address>();
      oldAddresses.add(a1);
      oldAddresses.add(a2);
      DefaultConsistentHashFactory chf = new DefaultConsistentHashFactory();
      DefaultConsistentHash oldCh = chf.create(MurmurHash3.getInstance(), 2, 3, oldAddresses, null);
      List<Address> newAddresses = new ArrayList<Address>();
      newAddresses.add(a1);
      newAddresses.add(a2);
      newAddresses.add(a3);
      DefaultConsistentHash newCh = chf.create(MurmurHash3.getInstance(), 2, 3, newAddresses, null);
      StateRequestCommand c14 = new StateRequestCommand(cacheName, StateRequestCommand.Type.START_STATE_TRANSFER, a1, 99, Collections.emptySet());
      byte[] bytes = marshaller.objectToByteBuffer(c14);
      marshaller.objectFromByteBuffer(bytes);

      bytes = marshaller.objectToByteBuffer(c14);
      marshaller.objectFromByteBuffer(bytes);

      bytes = marshaller.objectToByteBuffer(c14);
      marshaller.objectFromByteBuffer(bytes);
   }

   public void testInternalCacheEntryMarshalling() throws Exception {
      ImmortalCacheEntry entry1 = (ImmortalCacheEntry) TestInternalCacheEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, -1, System.currentTimeMillis(), -1);
      marshallAndAssertEquality(entry1);

      MortalCacheEntry entry2 = (MortalCacheEntry) TestInternalCacheEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, 200000, System.currentTimeMillis(), -1);
      marshallAndAssertEquality(entry2);

      TransientCacheEntry entry3 = (TransientCacheEntry) TestInternalCacheEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, -1, System.currentTimeMillis(), 4000000);
      marshallAndAssertEquality(entry3);

      TransientMortalCacheEntry entry4 = (TransientMortalCacheEntry) TestInternalCacheEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, 200000, System.currentTimeMillis(), 4000000);
      marshallAndAssertEquality(entry4);
   }

   public void testInternalCacheValueMarshalling() throws Exception {
      ImmortalCacheValue value1 = (ImmortalCacheValue) TestInternalCacheEntryFactory.createValue("value", System.currentTimeMillis() - 1000, -1, System.currentTimeMillis(), -1);
      byte[] bytes = marshaller.objectToByteBuffer(value1);
      ImmortalCacheValue rvalue1 = (ImmortalCacheValue) marshaller.objectFromByteBuffer(bytes);
      assert rvalue1.getValue().equals(value1.getValue()) : "Writen[" + rvalue1.getValue() + "] and read[" + value1.getValue() + "] objects should be the same";

      MortalCacheValue value2 = (MortalCacheValue) TestInternalCacheEntryFactory.createValue("value", System.currentTimeMillis() - 1000, 200000, System.currentTimeMillis(), -1);
      bytes = marshaller.objectToByteBuffer(value2);
      MortalCacheValue rvalue2 = (MortalCacheValue) marshaller.objectFromByteBuffer(bytes);
      assert rvalue2.getValue().equals(value2.getValue()) : "Writen[" + rvalue2.getValue() + "] and read[" + value2.getValue() + "] objects should be the same";

      TransientCacheValue value3 = (TransientCacheValue) TestInternalCacheEntryFactory.createValue("value", System.currentTimeMillis() - 1000, -1, System.currentTimeMillis(), 4000000);
      bytes = marshaller.objectToByteBuffer(value3);
      TransientCacheValue rvalue3 = (TransientCacheValue) marshaller.objectFromByteBuffer(bytes);
      assert rvalue3.getValue().equals(value3.getValue()) : "Writen[" + rvalue3.getValue() + "] and read[" + value3.getValue() + "] objects should be the same";

      TransientMortalCacheValue value4 = (TransientMortalCacheValue) TestInternalCacheEntryFactory.createValue("value", System.currentTimeMillis() - 1000, 200000, System.currentTimeMillis(), 4000000);
      bytes = marshaller.objectToByteBuffer(value4);
      TransientMortalCacheValue rvalue4 = (TransientMortalCacheValue) marshaller.objectFromByteBuffer(bytes);
      assert rvalue4.getValue().equals(value4.getValue()) : "Writen[" + rvalue4.getValue() + "] and read[" + value4.getValue() + "] objects should be the same";
   }

   public void testLongPutKeyValueCommand() throws Exception {
      PutKeyValueCommand c = new PutKeyValueCommand(
            "SESSION_173", "@TSXMHVROYNOFCJVEUJQGBCENNQDEWSCYSOHECJOHEICBEIGJVTIBB@TVNCWLTQCGTEJ@NBJLTMVGXCHXTSVE@BCRYGWPRVLXOJXBRJDVNBVXPRTRLBMHPOUYQKDEPDSADUAWPFSIOCINPSSFGABDUXRMTMMJMRTGBGBOAMGVMTKUDUAJGCAHCYW@LAXMDSFYOSXJXLUAJGQKPTHUKDOXRWKEFIVRTH@VIMQBGYPKWMS@HPOESTPIJE@OTOTWUWIOBLYKQQPTNGWVLRRCWHNIMWDQNOO@JHHEVYVQEODMWKFKKKSWURVDLXPTFQYIHLIM@GSBFWMDQGDQIJONNEVHGQTLDBRBML@BEWGHOQHHEBRFUQSLB@@CILXEAVQQBTXSITMBXHMHORHLTJF@MKMHQGHTSENWILTAKCCPVSQIPBVRAFSSEXIOVCPDXHUBIBUPBSCGPRECXEPMQHRHDOHIHVBPNDKOVLPCLKAJMNOTSF@SRXYVUEMQRCXVIETXVHOVNGYERBNM@RIMGHC@FNTUXSJSKALGHAFHGTFEANQUMBPUYFDSGLUYRRFDJHCW@JBWOBGMGTITAICRC@TPVCRKRMFPUSRRAHI@XOYKVGPHEBQD@@APEKSBCTBKREWAQGKHTJ@IHJD@YFSRDQPA@HKKELIJGFDYFEXFCOTCQIHKCQBLVDFHMGOWIDOWMVBDSJQOFGOIAPURRHVBGEJWYBUGGVHE@PU@NMQFMYTNYJDWPIADNVNCNYCCCPGODLAO@YYLVITEMNNKIFSDXKORJYWMFGKNYFPUQIC@AIDR@IWXCVALQBDOXRWIBXLKYTWDNHHSCUROAU@HVNENDAOP@RPTRIGLLLUNDQIDXJDDNF@P@PA@FEIBQKSKFQITTHDYGQRJMWPRLQC@NJVNVSKGOGYXPYSQHKPALKLFWNAOSQFTLEPVOII@RPDNRCVRDUMMFIVSWGIASUBMTGQSDGB@TBBYECFBRBGILJFCJ@JIQIQRVJXWIPGNVXKYATSPJTIPGCMCNPOKNEHBNUIAEQFQTYVLGAR@RVWVA@RMPBX@LRLJUEBUWO@PKXNIP@FKIQSVWKNO@FOJWDSIOLXHXJFBQPPVKKP@YKXPOOMBTLXMEHPRLLSFSVGMPXXNBCYVVSPNGMFBJUDCVOVGXPKVNTOFKVJUJOSDHSCOQRXOKBVP@WCUUFGMJAUQ@GRAGXICFCFICBSNASUBPAFRIPUK@OXOCCNOGTTSFVQKBQNB@DWGVEFSGTAXAPLBJ@SYHUNXWXPMR@KPFAJCIXPDURELFYPMUSLTJSQNDHHKJTIWCGNEKJF@CUWYTWLPNHYPHXNOGLSICKEFDULIXXSIGFMCQGURSRTUJDKRXBUUXIDFECMPXQX@CVYLDABEMFKUGBTBNMNBPCKCHWRJKSOGJFXMFYLLPUVUHBCNULEFAXPVKVKQKYCEFRUYPBRBDBDOVYLIQMQBLTUK@PRDCYBOKJGVUADFJFAFFXKJTNAJTHISWOSMVAYLIOGIORQQWFAKNU@KHPM@BYKTFSLSRHBATQTKUWSFAQS@Y@QIKCUWQYTODBRCYYYIAFMDVRURKVYJXHNGVLSQQFCXKLNUPCTEJSWIJUBFELSBUHANELHSIWLVQSSAIJRUEDOHHX@CKEBPOJRLRHEPLENSCDGEWXRTVUCSPFSAJUXDJOIUWFGPKHBVRVDMUUCPUDKRKVAXPSOBOPKPRRLFCKTLH@VGWKERASJYU@JAVWNBJGQOVF@QPSGJVEPAV@NAD@@FQRYPQIOAURILWXCKINPMBNUHPUID@YDQBHWAVDPPWRFKKGWJQTI@@OPSQ@ROUGHFNHCJBDFCHRLRTEMTUBWVCNOPYXKSSQDCXTOLOIIOCXBTPAUYDICFIXPJRB@CHFNXUCXANXYKXAISDSSLJGQOLBYXWHG@@KPARPCKOXAYVPDGRW@LDCRQBNMJREHWDYMXHEXAJQKHBIRAVHJQIVGOIXNINYQMJBXKM@DXESMBHLKHVSFDLVPOSOVMLHPSHQYY@DNMCGGGAJMHPVDLBGJP@EVDGLYBMD@NWHEYTBPIBPUPYOPOJVV@IVJXJMHIWWSIRKUWSR@U@@TDVMG@GRXVLCNEIISEVIVPOMJHKOWMRMITYDUQASWJIKVNYUFQVDT@BHTOMFXVFRKAARLNOGX@ADWCKHOVEMIGBWXINCUXEMVHSJJQDU@INTHDJQPSAQNAYONDBBFYGBTNGUSJHRKLCPHQMNLDHUQJPLLCDVTYLXTHJCBUXCRDY@YI@IQDCLJBBJC@NXGANXFIWPPNFVTDJWQ@@BIYJONOFP@RHTQEYPVHPPUS@UUENSNNF@WVGTSAVKDSQNMHP@VJORGTVWXVBPWKQNRWLSQFSBMXQKWRYMXPAYREXYGONKEWJMBCSLB@KSHXMIWMSBDGQWPDMUGVNMEWKMJKQECIRRVXBPBLGAFTUFHYSHLF@TGYETMDXRFAXVEUBSTGLSMWJMXJWMDPPDAFGNBMTQEMBDLRASMUMU@QTCDCPEGODHESDQVEIQYBJJPFXDLWPUNFAREYCY@YDDSTMKWCANNPXF@@WLMEXRPUNTWNOX@YKFNNTGMXIBBDA@TYLPJFNFHPQKMSNCLBME@FBPOIYNSDFBLHITKIFEFNXXOJAAFMRTGPALOANXF@YPY@RYTVOW@AKNM@C@LJKGBJMUYGGTXRHQCPOLNOGPPS@YSKAJSTQHLRBXUACXJYBLJSEHDNMLLUBSOIHQUI@VUNF@XAVRXUCYNCBDDGUDNVRYP@TPFPKGVNPTEDOTTUUFKCHQ@WWASQXLCBHNRBVSD@NVYT@GJQYSQGYPJO@WSEYDVKCBWANAFUWLDXOQYCYP@BSJFCBTXGKUNWLWUCYL@TNOWGDFHQTWQVYLQBBRQVMGNDBVXEFXTMMVYSHNVTTQAJCHKULOAJUSGJRPHQFCROWE@OMFUVRKGCWED@IAQGRLADOJGQKLCL@FCKTSITGMJRCCMPLOS@ONPQWFUROXYAUJQXIYVDCYBPYHPYCXNCRKRKLATLWWXLBLNOPUJFUJEDOIRKS@MMYPXIJNXPFOQJCHSCBEBGDUQYXQAWEEJDOSINXYLDXUJCQECU@WQSACTDFLGELHPGDFVDXFSSFOSYDLHQFVJESNAVAHKTUPBTPLSFSHYKLEXJXGWESVQQUTUPU@QXRTIDQ@IXBBOYINNHPEMTPRVRNJPQJFACFXUBKXOFHQSPOTLCQ@PLWGEFNKYCYFMKWPFUP@GLHKNMASGIENCACUISTG@YNQCNSOSBKOIXORKSHEOXHSMJJRUICJTCK@PWFRBPLXU@MUEMPFGDLUJEKD@ROUFBLKATXUCHEAQHEYDLCFDIRJSAXTV@CYMPQNMLTMFAHPRBLNSCVFBJMKQLAHWYIOLRMTOY@@RNKTUXHFYUMHGKCCGNEOIOQCISJEHCEVTTWM@TLFRIFDREHFBTTDEJRUNTWAEETGSVDOR@@UQNKFERMBVFJBOAYHPOKMSMRIERDA@JXYSJ@ORER@MBAVWCVGFNA@FRRPQSIIOIUGAJKVQXGINUUKPJPLQRMHPUBETEEIMIBPM@PETR@XD@DOHGRIBVXKLXQWHUFMTWEDYWFWRLPGDS@TANUXGIDTRVXKVCVEXYRKXQCTI@WNSFRAHJJGG@NIPPAAOJXQRTCLBYKDA@FFGHNUIGBFKOQMEDUEFELFLNKPCHA@OXJJRYNPDFSXIFSJYTDMSSBHDPUSQQDAVD@JAAWJDSVTERAJBFEPVRWKMYAPISPWLDPSRE@UMRQLXERTWRDLQVMVCOM@NYPXFLWMWKALMQVNJ@HCTMMIOLRWBJHCYFLMM@IWXPSHRRUNICSSWHOQHUVJE@HKJAADLBTPVLDAKCHRSURJCAXYTMYKHQMWDAWWASUW@HWGBVPTRHJGDWOGHPCNWSXTNKWONQGEKDDWGCKWVSAD@YLCCENMCHALHVDYQW@NQGNCY@M@GGV@RIR@OUS@PQIJMCFEIMGPYBXYR@NSIAUEXT@MOCNWRMLYHUUAFJCCLLRNFGKLPPIIH@BYRME@UJAKIFHOV@ILP@BGXRNJBIBARSOIMTDSHMGPIGRJBGHYRYXPFUHVOOMCQFNLM@CNCBTGO@UKXBOICNVCRGHADYQVAMNSFRONJ@WITET@BSHMQLWYMVGMQJVSJOXOUJDSXYVVBQJSVGREQLIQKWC@BMDNONHXFYPQENSJINQYKHVCTUTG@QQYJKJURDCKJTUQAM@DWNXWRNILYVAAJ@IADBIXKEIHVXLXUVMGQPAQTWJCDMVDVYUDTXQTCYXDPHKBAGMTAMKEM@QNOQJBREXNWFCXNXRPGOGEIR@KQJIGXAWXLTNCX@ID@XNRNYGRF@QPNWEX@XH@XKSXLQTLQPFSHAHXJLHUTNQWFFAJYHBWIFVJELDPSPLRRDPPNXSBYBEREEELIWNVYXOXYJQAIGHALUAWNUSSNMBHBFLRMMTKEKNSINECUGWTDNMROXI@BJJXKSPIIIXOAJBFVSITQDXTODBGKEPJMWK@JOL@SWTCGSHCOPHECTPJFUXIHUOSVMUTNNSLLJDEOMAGIXEAAVILRMOJXVHHPNPUYYODMXYAYGHI@BUB@NLP@KNPCYFRWAFES@WISBACDSPELEVTJEBNRVENSXXEVDVC@RIDIDSBPQIQNNSRPS@HCJ@XPIOFDXHUBCNFQKHMUYLXW@LMFMALHLESSXCOULRWDTJIVKKTLGFE@HKGVKUGMVHWACQOTSVNWBNUUGTMSQEJ@DXJQQYPOWVRQNQKXSLOEAA@@FRDCGCCQWQ@IY@EATGQGQIETPIJHOIQRYWLTGUENQYDNQSBI@IAUDEWDKICHNUGNAIXNICMBK@CJGSASMTFKWOBSI@KULNENWXV@VNFOANM@OJHFVV@IYRMDB@LHSGXIJMMFCGJKTKDXSMY@FHDNY@VSDUORGWVFMVKJXOCCDLSLMHCSXFBTW@RQTFNRDJUIKRD@PWPY",
            false, null, new EmbeddedMetadata.Builder().build(), EnumUtil.EMPTY_BIT_SET, AnyEquivalence.getInstance(), CommandInvocationId.generateId(null));
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
   }

   public void testNestedNonSerializable() throws Exception {
      PutKeyValueCommand cmd = new PutKeyValueCommand(
            "k", new Object(), false, null, new EmbeddedMetadata.Builder().build(),
            EnumUtil.EMPTY_BIT_SET, AnyEquivalence.getInstance(), CommandInvocationId.generateId(null));
      try {
         marshaller.objectToByteBuffer(cmd);
      } catch (NotSerializableException e) {
         log.info("Log exception for output format verification", e);
         TraceInformation inf = (TraceInformation) e.getCause();
         if (inf != null) {
            assert inf.toString().contains("in object java.lang.Object@");
            assert inf.toString().contains("in object org.infinispan.commands.write.PutKeyValueCommand@");
         }
      }
   }

   public void testNonSerializable() throws Exception {
      try {
         marshaller.objectToByteBuffer(new Object());
      } catch (NotSerializableException e) {
         log.info("Log exception for output format verification", e);
         TraceInformation inf = (TraceInformation) e.getCause();
         if (inf != null) {
            assert inf.toString().contains("in object java.lang.Object@");
         }
      }
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
         if (inf != null)
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

//   public void testPojoWithJBossMarshallingExternalizer(Method m) throws Exception {
//      PojoWithJBossExternalize pojo = new PojoWithJBossExternalize(27, k(m));
//      marshallAndAssertEquality(pojo);
//   }

   public void testErrorUnmarshallInputStreamAvailable() throws Exception {
      byte[] bytes = marshaller.objectToByteBuffer("23");
      Object o = marshaller.objectFromInputStream(new ByteArrayInputStream(bytes){
         @Override
         public int available() {
            return 0;
         }
      });
      assertEquals("23", o);
   }

   public void testFlagMarshalling() throws Exception {
      marshallAndAssertEquality(Arrays.asList(Flag.values()));
   }

   public void testSingleFlagMarshalling() throws Exception {
      marshallAndAssertEquality(Flag.FORCE_SYNCHRONOUS);
   }

   public void testEnumSetSingleElementMarshalling() throws Exception {
      marshallAndAssertEquality(EnumSet.of(Flag.FORCE_SYNCHRONOUS));
   }

   public void testEnumSetMultiElementMarshalling() throws Exception {
      marshallAndAssertEquality(EnumSet.of(Flag.FORCE_SYNCHRONOUS, Flag.FORCE_ASYNCHRONOUS));
   }

   public void testIsMarshallableSerializableWithAnnotation() throws Exception {
      PojoWithSerializeWith pojo = new PojoWithSerializeWith(17, "k1");
      assertTrue(marshaller.isMarshallable(pojo));
   }

//   public void testIsMarshallableJBossExternalizeAnnotation() throws Exception {
//      PojoWithJBossExternalize pojo = new PojoWithJBossExternalize(34, "k2");
//      assertTrue(marshaller.isMarshallable(pojo));
//   }

   public void testListArray() throws Exception {
      List<Integer>[] numbers = new List[]{Arrays.asList(1), Arrays.asList(2)};
      marshallAndAssertArrayEquality(numbers);
   }

   public void testByteArray() throws Exception {
      byte[] bytes = new byte[]{1, 2, 3};
      marshallAndAssertByteArrayEquality(bytes);
   }

   protected void marshallAndAssertEquality(Object writeObj) throws Exception {
      byte[] bytes = marshaller.objectToByteBuffer(writeObj);
      log.debugf("Payload size for object=%s : %s", writeObj, bytes.length);
      Object readObj = marshaller.objectFromByteBuffer(bytes);
      assert readObj.equals(writeObj) : "Writen[" + writeObj + "] and read[" + readObj + "] objects should be the same";
   }

   protected void marshallAndAssertEquality(ReplicableCommand writeObj) throws Exception {
      byte[] bytes = marshaller.objectToByteBuffer(writeObj);
      log.debugf("Payload size for object=%s : %s", writeObj, bytes.length);
      ReplicableCommand readObj = (ReplicableCommand) marshaller.objectFromByteBuffer(bytes);
      assert readObj.getCommandId() == writeObj.getCommandId() : "Writen[" + writeObj.getCommandId() + "] and read[" + readObj.getCommandId() + "] objects should be the same";
      assert readObj.equals(writeObj) : "Writen[" + writeObj + "] and read[" + readObj + "] objects should be the same";
   }

   protected void marshallAndAssertArrayEquality(Object[] writeObj) throws Exception {
      byte[] bytes = marshaller.objectToByteBuffer(writeObj);
      log.debugf("Payload size for object=%s : %s", Arrays.toString(writeObj), bytes.length);
      Object[] readObj = (Object[]) marshaller.objectFromByteBuffer(bytes);
      assertArrayEquals("Writen[" + Arrays.toString(writeObj) + "] and read["
            + Arrays.toString(readObj) + "] objects should be the same",
            readObj, writeObj);
   }

   protected void marshallAndAssertByteArrayEquality(byte[] writeObj) throws Exception {
      byte[] bytes = marshaller.objectToByteBuffer(writeObj);
      log.debugf("Payload size for byte[]=%s : %s", Util.toHexString(writeObj), bytes.length);
      byte[] readObj = (byte[]) marshaller.objectFromByteBuffer(bytes);
      assertArrayEquals("Writen[" + Util.toHexString(writeObj)+ "] and read["
            + Util.toHexString(readObj)+ "] objects should be the same",
            readObj, writeObj);
   }

   public static class Pojo implements Externalizable {
      int i;
      boolean b;
      static int serializationCount, deserializationCount;
      private static final long serialVersionUID = 9032309454840083326L;

      @Override
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

      @Override
      public int hashCode() {
         int result;
         result = i;
         result = 31 * result + (b ? 1 : 0);
         return result;
      }

      @Override
      public void writeExternal(ObjectOutput out) throws IOException {
         out.writeInt(i);
         out.writeBoolean(b);
         serializationCount++;
      }

      @Override
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

   static class Human implements Serializable {

      int age;

      Human age(int age) {
         this.age = age;
         return this;
      }

   }

   static class HumanComparator implements Comparator<Human>, Serializable {

      @Override
      public int compare(Human o1, Human o2) {
         if (o1.age < o2.age) return -1;
         if (o1.age == o2.age) return 0;
         return 1;
      }

   }

}
