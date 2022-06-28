package org.infinispan.marshall;

import static org.infinispan.test.TestingUtil.extractGlobalMarshaller;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.io.ByteArrayInputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
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

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.statetransfer.StateTransferStartCommand;
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
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.marshall.PojoWithSerializeWith;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.FastCopyHashMap;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheValue;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.data.BrokenMarshallingPojo;
import org.infinispan.test.data.Key;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.UUID;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "marshall.VersionAwareMarshallerTest")
public class VersionAwareMarshallerTest extends AbstractInfinispanTest {

   private static final Log log = LogFactory.getLog(VersionAwareMarshallerTest.class);
   protected StreamingMarshaller marshaller;
   private EmbeddedCacheManager cm;

   private final TransactionFactory gtf = new TransactionFactory();

   public VersionAwareMarshallerTest() {
      gtf.init(false, false, true, false);
   }

   @BeforeClass
   public void setUp() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration(), builder);
      marshaller = extractGlobalMarshaller(cm);
   }

   protected GlobalConfigurationBuilder globalConfiguration() {
      // Use a clustered cache manager to be able to test global marshaller interaction too
      GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalBuilder.serialization().addAdvancedExternalizer(new PojoWithExternalAndInternal.Externalizer());
      globalBuilder.serialization().addAdvancedExternalizer(new PojoWithExternalizer.Externalizer());
      globalBuilder.serialization().addAdvancedExternalizer(new PojoWithMultiExternalizer.Externalizer());
      globalBuilder.serialization().serialization().addContextInitializer(new VersionAwareMarshallerSCIImpl());
      return globalBuilder;
   }

   @AfterClass
   public void tearDown() {
      cm.stop();
   }

   public void testJGroupsAddressMarshalling() throws Exception {
      JGroupsAddress address = new JGroupsAddress(UUID.randomUUID());
      marshallAndAssertEquality(address);
   }

   public void testGlobalTransactionMarshalling() throws Exception {
      JGroupsAddress jGroupsAddress = new JGroupsAddress(UUID.randomUUID());
      GlobalTransaction gtx = gtf.newGlobalTransaction(jGroupsAddress, false);
      marshallAndAssertEquality(gtx);
   }

   public void testListMarshalling() throws Exception {
      List<GlobalTransaction> l1 = new ArrayList<>();
      List<GlobalTransaction> l2 = new LinkedList<>();
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
      Map<Integer, GlobalTransaction> m1 = new HashMap<>();
      Map<Integer, GlobalTransaction> m2 = new TreeMap<>();
      Map<Integer, GlobalTransaction> m3 = new HashMap<>();
      Map<Integer, GlobalTransaction> m4 = new FastCopyHashMap<>();
      for (int i = 0; i < 10; i++) {
         JGroupsAddress jGroupsAddress = new JGroupsAddress(UUID.randomUUID());
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
      Set<Integer> s1 = new HashSet<>();
      Set<Integer> s2 = new TreeSet<>();
      for (int i = 0; i < 10; i++) {
         Integer integ = 1000 * i;
         s1.add(integ);
         s2.add(integ);
      }
      marshallAndAssertEquality(s1);
      marshallAndAssertEquality(s2);
   }

   public void testSingletonListMarshalling() throws Exception {
      GlobalTransaction gtx = gtf.newGlobalTransaction(new JGroupsAddress(UUID.randomUUID()), false);
      List<GlobalTransaction> l = Collections.singletonList(gtx);
      marshallAndAssertEquality(l);
   }

   public void testImmutableResponseMarshalling() throws Exception {
      marshallAndAssertEquality(UnsuccessfulResponse.EMPTY);
      marshallAndAssertEquality(UnsureResponse.INSTANCE);
   }

   public void testReplicableCommandsMarshalling() throws Exception {
      ByteString cacheName = ByteString.fromString(TestingUtil.getDefaultCacheName(cm));
      ClusteredGetCommand c2 = new ClusteredGetCommand("key", cacheName, 0, EnumUtil.EMPTY_BIT_SET);
      marshallAndAssertEquality(c2);

      // SizeCommand does not have an empty constructor, so doesn't look to be one that is marshallable.

      GetKeyValueCommand c4 = new GetKeyValueCommand("key", 0, EnumUtil.EMPTY_BIT_SET);
      marshallAndAssertEquality(c4);

      PutKeyValueCommand c5 = new PutKeyValueCommand("k", "v", false, false,
            new EmbeddedMetadata.Builder().build(), 0, EnumUtil.EMPTY_BIT_SET, CommandInvocationId.generateId(null));
      marshallAndAssertEquality(c5);

      RemoveCommand c6 = new RemoveCommand("key", null, false, 0, EnumUtil.EMPTY_BIT_SET, CommandInvocationId.generateId(null));
      marshallAndAssertEquality(c6);

      // EvictCommand does not have an empty constructor, so doesn't look to be one that is marshallable.

      InvalidateCommand c7 = new InvalidateCommand(EnumUtil.EMPTY_BIT_SET, CommandInvocationId.generateId(null), "key1", "key2");
      marshallAndAssertEquality(c7);

      InvalidateCommand c71 = new InvalidateL1Command(EnumUtil.EMPTY_BIT_SET, CommandInvocationId.generateId(null), "key1", "key2");
      marshallAndAssertEquality(c71);

      ReplaceCommand c8 = new ReplaceCommand("key", "oldvalue", "newvalue", false, new EmbeddedMetadata.Builder().build(), 0,
            EnumUtil.EMPTY_BIT_SET, CommandInvocationId.generateId(null));
      marshallAndAssertEquality(c8);

      ClearCommand c9 = new ClearCommand();
      marshallAndAssertEquality(c9);

      Map<Integer, GlobalTransaction> m1 = new HashMap<>();
      for (int i = 0; i < 10; i++) {
         GlobalTransaction gtx = gtf.newGlobalTransaction(new JGroupsAddress(UUID.randomUUID()), false);
         m1.put(1000 * i, gtx);
      }
      PutMapCommand c10 = new PutMapCommand(m1, new EmbeddedMetadata.Builder().build(), EnumUtil.EMPTY_BIT_SET, CommandInvocationId.generateId(null));
      marshallAndAssertEquality(c10);

      Address local = new JGroupsAddress(UUID.randomUUID());
      GlobalTransaction gtx = gtf.newGlobalTransaction(local, false);
      PrepareCommand c11 = new PrepareCommand(cacheName, gtx, Arrays.asList(c5, c6, c8, c10), true);
      marshallAndAssertEquality(c11);

      CommitCommand c12 = new CommitCommand(cacheName, gtx);
      marshallAndAssertEquality(c12);

      RollbackCommand c13 = new RollbackCommand(cacheName, gtx);
      marshallAndAssertEquality(c13);
   }

   public void testStateTransferControlCommand() throws Exception {
      ByteString cacheName = ByteString.fromString(TestingUtil.getDefaultCacheName(cm));
      ImmortalCacheEntry entry1 = (ImmortalCacheEntry) TestInternalCacheEntryFactory.create("key", "value", System.currentTimeMillis() - 1000, -1, System.currentTimeMillis(), -1);
      Collection<InternalCacheEntry> state = new ArrayList<>();
      state.add(entry1);
      Address a1 = new JGroupsAddress(UUID.randomUUID());
      Address a2 = new JGroupsAddress(UUID.randomUUID());
      Address a3 = new JGroupsAddress(UUID.randomUUID());
      List<Address> oldAddresses = new ArrayList<>();
      oldAddresses.add(a1);
      oldAddresses.add(a2);
      List<Address> newAddresses = new ArrayList<>();
      newAddresses.add(a1);
      newAddresses.add(a2);
      newAddresses.add(a3);
      CacheRpcCommand c14 = new StateTransferStartCommand(cacheName, 99, IntSets.mutableEmptySet());
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
            false, false, new EmbeddedMetadata.Builder().build(), 0, EnumUtil.EMPTY_BIT_SET,
            CommandInvocationId.generateId(null));
      marshallAndAssertEquality(c);
   }

   public void testExceptionResponse() throws Exception {
      ExceptionResponse er = new ExceptionResponse(new TimeoutException());
      byte[] bytes = marshaller.objectToByteBuffer(er);
      ExceptionResponse rer = (ExceptionResponse) marshaller.objectFromByteBuffer(bytes);
      assert rer.getException().getClass().equals(er.getException().getClass()) : "Writen[" + er.getException().getClass() + "] and read[" + rer.getException().getClass() + "] objects should be the same";
   }

   @Test(expectedExceptions = MarshallingException.class)
   public void testNestedNonMarshallable() throws Exception {
      PutKeyValueCommand cmd = new PutKeyValueCommand("k", new Object(), false, false,
            new EmbeddedMetadata.Builder().build(), 0, EnumUtil.EMPTY_BIT_SET, CommandInvocationId.generateId(null));
      marshaller.objectToByteBuffer(cmd);
   }

   @Test(expectedExceptions = MarshallingException.class)
   public void testNonMarshallable() throws Exception {
      marshaller.objectToByteBuffer(new Object());
   }

   public void testConcurrentHashMap() throws Exception {
      ConcurrentHashMap<Integer, String> map = new ConcurrentHashMap<>();
      map.put(1, "v1");
      map.put(2, "v2");
      map.put(3, "v3");
      marshallAndAssertEquality(map);
   }

   public static class PojoWhichFailsOnUnmarshalling extends Pojo {
      private static final long serialVersionUID = -5109779096242560884L;

      public PojoWhichFailsOnUnmarshalling() {
         super(0, false);
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         throw new IOException("Injected failue!");
      }

   }

   @Test(expectedExceptions = MarshallingException.class)
   public void testErrorUnmarshalling() throws Exception {
      BrokenMarshallingPojo bmp = new BrokenMarshallingPojo(false);
      byte[] bytes = marshaller.objectToByteBuffer(bmp);
      marshaller.objectFromByteBuffer(bytes);
   }

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

   public void testSerializableWithAnnotation() throws Exception {
      marshallAndAssertEquality(new PojoWithSerializeWith(20, "k2"));
   }

   public void testListArray() throws Exception {
      List<Integer>[] numbers = new List[]{Arrays.asList(1), Arrays.asList(2)};
      marshallAndAssertArrayEquality(numbers);
   }

   public void testByteArray() throws Exception {
      byte[] bytes = new byte[]{1, 2, 3};
      marshallAndAssertByteArrayEquality(bytes);
   }

   public void testExternalAndInternalWithOffset() throws Exception {
      PojoWithExternalAndInternal obj = new PojoWithExternalAndInternal(new Human().age(23), "value");

      byte[] bytes = marshaller.objectToByteBuffer(obj);
      bytes = prependBytes(new byte[]{1, 2, 3}, bytes);

      Object readObj = marshaller.objectFromByteBuffer(bytes, 3, bytes.length);
      assertEquals(obj, readObj);
   }

   public void testArrays() throws Exception {
      marshallAndAssertArrayEquality(new Object[] { });
      marshallAndAssertArrayEquality(new String[] { null, "foo" });
      marshallAndAssertArrayEquality(new String[] { "foo", "bar" });
      marshallAndAssertArrayEquality(new Object[] { 1.2, 3.4 });
      marshallAndAssertArrayEquality(new Pojo[] { });
      marshallAndAssertArrayEquality(new Pojo[] { null });
      marshallAndAssertArrayEquality(new Pojo[] { null, null });
      marshallAndAssertArrayEquality(new Pojo[] { new Pojo(1, false), new Pojo(2, true) });
      marshallAndAssertArrayEquality(new Pojo[] { new Pojo(3, false), null });
      marshallAndAssertArrayEquality(new Pojo[] { new Pojo(4, false), new PojoExtended(5, true) });
      marshallAndAssertArrayEquality(new I[] { new Pojo(6, false), new Pojo(7, true) });
      marshallAndAssertArrayEquality(new I[] { new Pojo(8, false), new PojoExtended(9, true) });
      marshallAndAssertArrayEquality(new I[] { new Pojo(10, false), new PojoWithExternalizer(11, false) });
      marshallAndAssertArrayEquality(new PojoWithExternalizer[] {
         new PojoWithExternalizer(12, true), new PojoWithExternalizer(13, false) });
      marshallAndAssertArrayEquality(new I[] { new PojoWithExternalizer(14, false), new PojoWithExternalizer(15, true)});
      marshallAndAssertArrayEquality(new PojoWithMultiExternalizer[] {
            new PojoWithMultiExternalizer(16, true), new PojoWithMultiExternalizer(17, false) });
      marshallAndAssertArrayEquality(new I[] { new PojoWithMultiExternalizer(18, false), new PojoWithExternalizer(19, true)});
      marshallAndAssertArrayEquality(new I[] { new PojoWithMultiExternalizer(20, false), new PojoWithMultiExternalizer(21, true)});
      marshallAndAssertArrayEquality(new Object[] { new PojoWithMultiExternalizer(22, false), new PojoWithMultiExternalizer(23, true)});
      marshallAndAssertArrayEquality(new Object[] { new PojoWithExternalizer(24, false), new PojoWithExternalizer(25, true)});
      marshallAndAssertArrayEquality(new Object[] { new PojoAnnotated(26, false), "foo"});
      marshallAndAssertArrayEquality(new Object[] { new PojoAnnotated(27, false), new PojoAnnotated(28, true)});
      marshallAndAssertArrayEquality(new PojoAnnotated[] { new PojoAnnotated(27, false), new PojoAnnotated(28, true)});
      marshallAndAssertArrayEquality(new PojoAnnotated[] { null, null });
   }

   public void testLongArrays() throws Exception {
      for (int length : new int[] { 0xFF, 0x100, 0x101, 0x102, 0x100FF, 0x10100, 0x10101, 0x10102 }) {
         String[] array = new String[length];
         // test null
         marshallAndAssertArrayEquality(array);

         // test filled
         Arrays.fill(array, "a");
         marshallAndAssertArrayEquality(array);
      }
   }

   byte[] prependBytes(byte[] bytes, byte[] src) {
      byte[] res = new byte[bytes.length + src.length];
      System.arraycopy(bytes, 0, res, 0, bytes.length);
      System.arraycopy(src, 0, res, bytes.length, src.length);
      return res;
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
      log.debugf("Payload size for %s[]=%s : %s", writeObj.getClass().getComponentType().getName(), Arrays.toString(writeObj), bytes.length);
      Object[] readObj = (Object[]) marshaller.objectFromByteBuffer(bytes);
      assertArrayEquals("Writen[" + Arrays.toString(writeObj) + "] and read["
            + Arrays.toString(readObj) + "] objects should be the same",
            writeObj, readObj);
   }

   protected void marshallAndAssertByteArrayEquality(byte[] writeObj) throws Exception {
      byte[] bytes = marshaller.objectToByteBuffer(writeObj);
      log.debugf("Payload size for byte[]=%s : %s", Util.toHexString(writeObj), bytes.length);
      byte[] readObj = (byte[]) marshaller.objectFromByteBuffer(bytes);
      assertArrayEquals("Writen[" + Util.toHexString(writeObj)+ "] and read["
            + Util.toHexString(readObj)+ "] objects should be the same",
            writeObj, readObj);
   }

   public interface I {
   }

   public static class Pojo implements I, Externalizable {
      private static final long serialVersionUID = 9032309454840083326L;

      @ProtoField(number = 1, defaultValue = "0")
      int i;

      @ProtoField(number = 2, defaultValue = "false")
      boolean b;

      public Pojo() {}

      public Pojo(int i, boolean b) {
         this.i = i;
         this.b = b;
      }

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
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         i = in.readInt();
         b = in.readBoolean();
      }
   }

   @SerializeWith(PojoAnnotated.Externalizer.class)
   public static class PojoAnnotated extends Pojo {
      public PojoAnnotated(int i, boolean b) {
         super(i, b);
      }

      public static class Externalizer implements org.infinispan.commons.marshall.Externalizer<PojoAnnotated> {
         @Override
         public void writeObject(ObjectOutput output, PojoAnnotated object) throws IOException {
            output.writeInt(object.i);
            output.writeBoolean(object.b);
         }

         @Override
         public PojoAnnotated readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new PojoAnnotated(input.readInt(), input.readBoolean());
         }
      }
   }

   public static class PojoExtended extends Pojo {
      public PojoExtended() {}

      public PojoExtended(int i, boolean b) {
         super(i, b);
      }
   }

   public static class PojoWithExternalizer extends Pojo {

      public PojoWithExternalizer(int i, boolean b) {
         super(i, b);
      }

      public static class Externalizer implements AdvancedExternalizer<PojoWithExternalizer> {
         @Override
         public Set<Class<? extends PojoWithExternalizer>> getTypeClasses() {
            return Util.asSet(PojoWithExternalizer.class);
         }

         @Override
         public Integer getId() {
            return 1234;
         }

         @Override
         public void writeObject(ObjectOutput output, PojoWithExternalizer object) throws IOException {
            output.writeInt(object.i);
            output.writeBoolean(object.b);
         }

         @Override
         public PojoWithExternalizer readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new PojoWithExternalizer(input.readInt(), input.readBoolean());
         }
      }
   }

   public static class PojoWithMultiExternalizer extends Pojo {

      public PojoWithMultiExternalizer(int i, boolean b) {
         super(i, b);
      }

      public static class Externalizer implements AdvancedExternalizer<Object> {
         @Override
         public Set<Class<?>> getTypeClasses() {
            return Util.asSet(PojoWithMultiExternalizer.class, Thread.class);
         }

         @Override
         public Integer getId() {
            return 4321;
         }

         @Override
         public void writeObject(ObjectOutput output, Object o) throws IOException {
            PojoWithMultiExternalizer pojo = (PojoWithMultiExternalizer) o;
            output.writeInt(pojo.i);
            output.writeBoolean(pojo.b);
         }

         @Override
         public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new PojoWithMultiExternalizer(input.readInt(), input.readBoolean());
         }
      }
   }

   public static class Human implements Serializable {

      @ProtoField(number = 1, defaultValue = "0")
      int age;

      public Human() {}

      public Human age(int age) {
         this.age = age;
         return this;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Human human = (Human) o;

         return age == human.age;

      }

      @Override
      public int hashCode() {
         return age;
      }
   }

   public static class HumanComparator implements Comparator<Human>, Serializable {

      @Override
      public int compare(Human o1, Human o2) {
         if (o1.age < o2.age) return -1;
         if (o1.age == o2.age) return 0;
         return 1;
      }

   }

   public static class PojoWithExternalAndInternal {

      @ProtoField(1)
      final Human human;

      @ProtoField(2)
      final String value;

      @ProtoFactory
      PojoWithExternalAndInternal(Human human, String value) {
         this.human = human;
         this.value = value;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         PojoWithExternalAndInternal that = (PojoWithExternalAndInternal) o;

         if (!human.equals(that.human)) return false;
         return value.equals(that.value);

      }

      @Override
      public int hashCode() {
         int result = human.hashCode();
         result = 31 * result + value.hashCode();
         return result;
      }

      public static class Externalizer implements AdvancedExternalizer<PojoWithExternalAndInternal> {

         @Override
         public void writeObject(ObjectOutput out, PojoWithExternalAndInternal obj) throws IOException {
            out.writeObject(obj.human);
            out.writeObject(obj.value);
         }

         @Override
         public PojoWithExternalAndInternal readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            Human human = (Human) input.readObject();
            String value = (String) input.readObject();
            return new PojoWithExternalAndInternal(human, value);
         }

         @Override
         public Set<Class<? extends PojoWithExternalAndInternal>> getTypeClasses() {
            return Collections.singleton(PojoWithExternalAndInternal.class);
         }

         @Override
         public Integer getId() {
            return 999;
         }
      }
   }

   @AutoProtoSchemaBuilder(
         includeClasses = {
               Key.class,
               VersionAwareMarshallerTest.Human.class,
               VersionAwareMarshallerTest.Pojo.class,
               VersionAwareMarshallerTest.PojoExtended.class,
               VersionAwareMarshallerTest.PojoWithExternalAndInternal.class
         },
         schemaFileName = "test.core.VersionAwareMarshallerTest.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.core.VersionAwareMarshallerTest",
         service = false
   )
   interface VersionAwareMarshallerSCI extends SerializationContextInitializer {
   }
}
