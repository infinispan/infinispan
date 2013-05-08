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
package org.infinispan.loaders.file;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.io.UnclosableObjectInputStream;
import org.infinispan.io.UnclosableObjectOutputStream;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Test(groups = "unit", testName = "loaders.file.FileCacheStoreTest")
public class FileCacheStoreTest extends BaseCacheStoreTest {

   FileCacheStore fcs;
   String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this);
   }

   @AfterClass
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected CacheStore createCacheStore() throws CacheLoaderException {
      clearTempDir();
      fcs = new FileCacheStore();
      FileCacheStoreConfig cfg = new FileCacheStoreConfig()
         .fetchPersistentState(true)
         .fsyncMode(getFsyncMode())
         .location(tmpDirectory)
         .purgeSynchronously(true); // for more accurate unit testing
      fcs.init(cfg, getCache(), getMarshaller());
      fcs.start();
      return fcs;
   }

   protected FileCacheStoreConfig.FsyncMode getFsyncMode() {
      return FileCacheStoreConfig.FsyncMode.DEFAULT;
   }

   @Override
   public void testPreload() throws Exception {
      createUnrelatedFile();
      super.testPreload();
   }

   @Override
   public void testPurgeExpired() throws Exception {
      long start = now();
      long lifespan = 1000;
      cs.store(TestInternalCacheEntryFactory.create("k1", "v1", lifespan));
      cs.store(TestInternalCacheEntryFactory.create("k2", "v2", lifespan));
      cs.store(TestInternalCacheEntryFactory.create("k3", "v3", lifespan));
      assert cs.containsKey("k1") || moreThanLifespanElapsed(start, lifespan);
      assert cs.containsKey("k2") || moreThanLifespanElapsed(start, lifespan);
      assert cs.containsKey("k3") || moreThanLifespanElapsed(start, lifespan);
      createUnrelatedFile();
      Thread.sleep(lifespan + 100);
      cs.purgeExpired();
      FileCacheStore fcs = (FileCacheStore) cs;
      assert fcs.load("k1") == null;
      assert fcs.load("k2") == null;
      assert fcs.load("k3") == null;
   }

   private boolean moreThanLifespanElapsed(long start, long lifespan) {
      return now() - lifespan >= start;
   }

   private long now() {
      return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
   }

   private void createUnrelatedFile() throws IOException {
      File cacheStoreDirectory = new File(tmpDirectory);
      assert cacheStoreDirectory.exists();
      assert cacheStoreDirectory.canWrite();
      String newfile = cacheStoreDirectory.getAbsolutePath() + File.separator + "mockCache-" + this.getClass().getName() + File.separator + "externalExistingFile";
      File file = new File(newfile);
      boolean created = file.createNewFile();
      assert created;
   }

   public void testBucketRemoval() throws Exception {
      Bucket b;
      InternalCacheEntry se = TestInternalCacheEntryFactory.create("test", "value");
      fcs.store(se);
      b = fcs.loadBucketContainingKey("test");
      assert b != null;

      assert !b.getEntries().isEmpty();

      assert new File(fcs.root, b.getBucketIdAsString()).exists();

      b.removeEntry("test");
      assert b.getEntries().isEmpty();

      fcs.updateBucket(b);
      checkBucketExists(b);
   }

   public void testCacheStoreRebootable() throws Exception {
      String key = "testCacheStoreRebootable";
      InternalCacheEntry se = TestInternalCacheEntryFactory.create(key, "initialValue");
      fcs.store(se);
      Bucket b = fcs.loadBucketContainingKey(key);

      //stop and restart it:
      fcs.stop();
      fcs.start();

      InternalCacheEntry entry = b.getEntry(key);
      entry.setValue("updatedValue");
      fcs.updateBucket(b);
      assert "updatedValue".equals(fcs.load(key).getValue());
   }

   protected void checkBucketExists(Bucket b) {
      File file = new File(fcs.root, b.getBucketIdAsString());
      assert file.exists();
      assert file.length() == 0;
   }

   public void testToStream() throws Exception {
      cs.store(TestInternalCacheEntryFactory.create("k1", "v1", -1, -1));

      StreamingMarshaller marshaller = getMarshaller();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ObjectOutput oo = marshaller.startObjectOutput(out, false, 12);
      try {
         cs.toStream(new UnclosableObjectOutputStream(oo));
      } finally {
         marshaller.finishObjectOutput(oo);
         out.close();
      }

      ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
      ObjectInput oi = marshaller.startObjectInput(in, false);
      try {
         assert oi.readInt() == 1 : "we have 3 different buckets";
         assert oi.readObject().equals(fcs.getLockFromKey("k1") + "");
         assert oi.readInt() > 0; //size on disk
      } finally {
         marshaller.finishObjectInput(oi);
      }
   }

   public void testLongKeyValuesToStream() throws Exception {
      String k1 = "SESSION_173";
      String v1 = "@TSXMHVROYNOFCJVEUJQGBCENNQDEWSCYSOHECJOHEICBEIGJVTIBB@TVNCWLTQCGTEJ@NBJLTMVGXCHXTSVE@BCRYGWPRVLXOJXBRJDVNBVXPRTRLBMHPOUYQKDEPDSADUAWPFSIOCINPSSFGABDUXRMTMMJMRTGBGBOAMGVMTKUDUAJGCAHCYW@LAXMDSFYOSXJXLUAJGQKPTHUKDOXRWKEFIVRTH@VIMQBGYPKWMS@HPOESTPIJE@OTOTWUWIOBLYKQQPTNGWVLRRCWHNIMWDQNOO@JHHEVYVQEODMWKFKKKSWURVDLXPTFQYIHLIM@GSBFWMDQGDQIJONNEVHGQTLDBRBML@BEWGHOQHHEBRFUQSLB@@CILXEAVQQBTXSITMBXHMHORHLTJF@MKMHQGHTSENWILTAKCCPVSQIPBVRAFSSEXIOVCPDXHUBIBUPBSCGPRECXEPMQHRHDOHIHVBPNDKOVLPCLKAJMNOTSF@SRXYVUEMQRCXVIETXVHOVNGYERBNM@RIMGHC@FNTUXSJSKALGHAFHGTFEANQUMBPUYFDSGLUYRRFDJHCW@JBWOBGMGTITAICRC@TPVCRKRMFPUSRRAHI@XOYKVGPHEBQD@@APEKSBCTBKREWAQGKHTJ@IHJD@YFSRDQPA@HKKELIJGFDYFEXFCOTCQIHKCQBLVDFHMGOWIDOWMVBDSJQOFGOIAPURRHVBGEJWYBUGGVHE@PU@NMQFMYTNYJDWPIADNVNCNYCCCPGODLAO@YYLVITEMNNKIFSDXKORJYWMFGKNYFPUQIC@AIDR@IWXCVALQBDOXRWIBXLKYTWDNHHSCUROAU@HVNENDAOP@RPTRIGLLLUNDQIDXJDDNF@P@PA@FEIBQKSKFQITTHDYGQRJMWPRLQC@NJVNVSKGOGYXPYSQHKPALKLFWNAOSQFTLEPVOII@RPDNRCVRDUMMFIVSWGIASUBMTGQSDGB@TBBYECFBRBGILJFCJ@JIQIQRVJXWIPGNVXKYATSPJTIPGCMCNPOKNEHBNUIAEQFQTYVLGAR@RVWVA@RMPBX@LRLJUEBUWO@PKXNIP@FKIQSVWKNO@FOJWDSIOLXHXJFBQPPVKKP@YKXPOOMBTLXMEHPRLLSFSVGMPXXNBCYVVSPNGMFBJUDCVOVGXPKVNTOFKVJUJOSDHSCOQRXOKBVP@WCUUFGMJAUQ@GRAGXICFCFICBSNASUBPAFRIPUK@OXOCCNOGTTSFVQKBQNB@DWGVEFSGTAXAPLBJ@SYHUNXWXPMR@KPFAJCIXPDURELFYPMUSLTJSQNDHHKJTIWCGNEKJF@CUWYTWLPNHYPHXNOGLSICKEFDULIXXSIGFMCQGURSRTUJDKRXBUUXIDFECMPXQX@CVYLDABEMFKUGBTBNMNBPCKCHWRJKSOGJFXMFYLLPUVUHBCNULEFAXPVKVKQKYCEFRUYPBRBDBDOVYLIQMQBLTUK@PRDCYBOKJGVUADFJFAFFXKJTNAJTHISWOSMVAYLIOGIORQQWFAKNU@KHPM@BYKTFSLSRHBATQTKUWSFAQS@Y@QIKCUWQYTODBRCYYYIAFMDVRURKVYJXHNGVLSQQFCXKLNUPCTEJSWIJUBFELSBUHANELHSIWLVQSSAIJRUEDOHHX@CKEBPOJRLRHEPLENSCDGEWXRTVUCSPFSAJUXDJOIUWFGPKHBVRVDMUUCPUDKRKVAXPSOBOPKPRRLFCKTLH@VGWKERASJYU@JAVWNBJGQOVF@QPSGJVEPAV@NAD@@FQRYPQIOAURILWXCKINPMBNUHPUID@YDQBHWAVDPPWRFKKGWJQTI@@OPSQ@ROUGHFNHCJBDFCHRLRTEMTUBWVCNOPYXKSSQDCXTOLOIIOCXBTPAUYDICFIXPJRB@CHFNXUCXANXYKXAISDSSLJGQOLBYXWHG@@KPARPCKOXAYVPDGRW@LDCRQBNMJREHWDYMXHEXAJQKHBIRAVHJQIVGOIXNINYQMJBXKM@DXESMBHLKHVSFDLVPOSOVMLHPSHQYY@DNMCGGGAJMHPVDLBGJP@EVDGLYBMD@NWHEYTBPIBPUPYOPOJVV@IVJXJMHIWWSIRKUWSR@U@@TDVMG@GRXVLCNEIISEVIVPOMJHKOWMRMITYDUQASWJIKVNYUFQVDT@BHTOMFXVFRKAARLNOGX@ADWCKHOVEMIGBWXINCUXEMVHSJJQDU@INTHDJQPSAQNAYONDBBFYGBTNGUSJHRKLCPHQMNLDHUQJPLLCDVTYLXTHJCBUXCRDY@YI@IQDCLJBBJC@NXGANXFIWPPNFVTDJWQ@@BIYJONOFP@RHTQEYPVHPPUS@UUENSNNF@WVGTSAVKDSQNMHP@VJORGTVWXVBPWKQNRWLSQFSBMXQKWRYMXPAYREXYGONKEWJMBCSLB@KSHXMIWMSBDGQWPDMUGVNMEWKMJKQECIRRVXBPBLGAFTUFHYSHLF@TGYETMDXRFAXVEUBSTGLSMWJMXJWMDPPDAFGNBMTQEMBDLRASMUMU@QTCDCPEGODHESDQVEIQYBJJPFXDLWPUNFAREYCY@YDDSTMKWCANNPXF@@WLMEXRPUNTWNOX@YKFNNTGMXIBBDA@TYLPJFNFHPQKMSNCLBME@FBPOIYNSDFBLHITKIFEFNXXOJAAFMRTGPALOANXF@YPY@RYTVOW@AKNM@C@LJKGBJMUYGGTXRHQCPOLNOGPPS@YSKAJSTQHLRBXUACXJYBLJSEHDNMLLUBSOIHQUI@VUNF@XAVRXUCYNCBDDGUDNVRYP@TPFPKGVNPTEDOTTUUFKCHQ@WWASQXLCBHNRBVSD@NVYT@GJQYSQGYPJO@WSEYDVKCBWANAFUWLDXOQYCYP@BSJFCBTXGKUNWLWUCYL@TNOWGDFHQTWQVYLQBBRQVMGNDBVXEFXTMMVYSHNVTTQAJCHKULOAJUSGJRPHQFCROWE@OMFUVRKGCWED@IAQGRLADOJGQKLCL@FCKTSITGMJRCCMPLOS@ONPQWFUROXYAUJQXIYVDCYBPYHPYCXNCRKRKLATLWWXLBLNOPUJFUJEDOIRKS@MMYPXIJNXPFOQJCHSCBEBGDUQYXQAWEEJDOSINXYLDXUJCQECU@WQSACTDFLGELHPGDFVDXFSSFOSYDLHQFVJESNAVAHKTUPBTPLSFSHYKLEXJXGWESVQQUTUPU@QXRTIDQ@IXBBOYINNHPEMTPRVRNJPQJFACFXUBKXOFHQSPOTLCQ@PLWGEFNKYCYFMKWPFUP@GLHKNMASGIENCACUISTG@YNQCNSOSBKOIXORKSHEOXHSMJJRUICJTCK@PWFRBPLXU@MUEMPFGDLUJEKD@ROUFBLKATXUCHEAQHEYDLCFDIRJSAXTV@CYMPQNMLTMFAHPRBLNSCVFBJMKQLAHWYIOLRMTOY@@RNKTUXHFYUMHGKCCGNEOIOQCISJEHCEVTTWM@TLFRIFDREHFBTTDEJRUNTWAEETGSVDOR@@UQNKFERMBVFJBOAYHPOKMSMRIERDA@JXYSJ@ORER@MBAVWCVGFNA@FRRPQSIIOIUGAJKVQXGINUUKPJPLQRMHPUBETEEIMIBPM@PETR@XD@DOHGRIBVXKLXQWHUFMTWEDYWFWRLPGDS@TANUXGIDTRVXKVCVEXYRKXQCTI@WNSFRAHJJGG@NIPPAAOJXQRTCLBYKDA@FFGHNUIGBFKOQMEDUEFELFLNKPCHA@OXJJRYNPDFSXIFSJYTDMSSBHDPUSQQDAVD@JAAWJDSVTERAJBFEPVRWKMYAPISPWLDPSRE@UMRQLXERTWRDLQVMVCOM@NYPXFLWMWKALMQVNJ@HCTMMIOLRWBJHCYFLMM@IWXPSHRRUNICSSWHOQHUVJE@HKJAADLBTPVLDAKCHRSURJCAXYTMYKHQMWDAWWASUW@HWGBVPTRHJGDWOGHPCNWSXTNKWONQGEKDDWGCKWVSAD@YLCCENMCHALHVDYQW@NQGNCY@M@GGV@RIR@OUS@PQIJMCFEIMGPYBXYR@NSIAUEXT@MOCNWRMLYHUUAFJCCLLRNFGKLPPIIH@BYRME@UJAKIFHOV@ILP@BGXRNJBIBARSOIMTDSHMGPIGRJBGHYRYXPFUHVOOMCQFNLM@CNCBTGO@UKXBOICNVCRGHADYQVAMNSFRONJ@WITET@BSHMQLWYMVGMQJVSJOXOUJDSXYVVBQJSVGREQLIQKWC@BMDNONHXFYPQENSJINQYKHVCTUTG@QQYJKJURDCKJTUQAM@DWNXWRNILYVAAJ@IADBIXKEIHVXLXUVMGQPAQTWJCDMVDVYUDTXQTCYXDPHKBAGMTAMKEM@QNOQJBREXNWFCXNXRPGOGEIR@KQJIGXAWXLTNCX@ID@XNRNYGRF@QPNWEX@XH@XKSXLQTLQPFSHAHXJLHUTNQWFFAJYHBWIFVJELDPSPLRRDPPNXSBYBEREEELIWNVYXOXYJQAIGHALUAWNUSSNMBHBFLRMMTKEKNSINECUGWTDNMROXI@BJJXKSPIIIXOAJBFVSITQDXTODBGKEPJMWK@JOL@SWTCGSHCOPHECTPJFUXIHUOSVMUTNNSLLJDEOMAGIXEAAVILRMOJXVHHPNPUYYODMXYAYGHI@BUB@NLP@KNPCYFRWAFES@WISBACDSPELEVTJEBNRVENSXXEVDVC@RIDIDSBPQIQNNSRPS@HCJ@XPIOFDXHUBCNFQKHMUYLXW@LMFMALHLESSXCOULRWDTJIVKKTLGFE@HKGVKUGMVHWACQOTSVNWBNUUGTMSQEJ@DXJQQYPOWVRQNQKXSLOEAA@@FRDCGCCQWQ@IY@EATGQGQIETPIJHOIQRYWLTGUENQYDNQSBI@IAUDEWDKICHNUGNAIXNICMBK@CJGSASMTFKWOBSI@KULNENWXV@VNFOANM@OJHFVV@IYRMDB@LHSGXIJMMFCGJKTKDXSMY@FHDNY@VSDUORGWVFMVKJXOCCDLSLMHCSXFBTW@RQTFNRDJUIKRD@PWPY@TSXMHVROYNOFCJVEUJQGBCENNQDEWSCYSOHECJOHEICBEIGJVTIBB@TVNCWLTQCGTEJ@NBJLTMVGXCHXTSVE@BCRYGWPRVLXOJXBRJDVNBVXPRTRLBMHPOUYQKDEPDSADUAWPFSIOCINPSSFGABDUXRMTMMJMRTGBGBOAMGVMTKUDUAJGCAHCYW@LAXMDSFYOSXJXLUAJGQKPTHUKDOXRWKEFIVRTH@VIMQBGYPKWMS@HPOESTPIJE@OTOTWUWIOBLYKQQPTNGWVLRRCWHNIMWDQNOO@JHHEVYVQEODMWKFKKKSWURVDLXPTFQYIHLIM@GSBFWMDQGDQIJONNEVHGQTLDBRBML@BEWGHOQHHEBRFUQSLB@@CILXEAVQQBTXSITMBXHMHORHLTJF@MKMHQGHTSENWILTAKCCPVSQIPBVRAFSSEXIOVCPDXHUBIBUPBSCGPRECXEPMQHRHDOHIHVBPNDKOVLPCLKAJMNOTSF@SRXYVUEMQRCXVIETXVHOVNGYERBNM@RIMGHC@FNTUXSJSKALGHAFHGTFEANQUMBPUYFDSGLUYRRFDJHCW@JBWOBGMGTITAICRC@TPVCRKRMFPUSRRAHI@XOYKVGPHEBQD@@APEKSBCTBKREWAQGKHTJ@IHJD@YFSRDQPA@HKKELIJGFDYFEXFCOTCQIHKCQBLVDFHMGOWIDOWMVBDSJQOFGOIAPURRHVBGEJWYBUGGVHE@PU@NMQFMYTNYJDWPIADNVNCNYCCCPGODLAO@YYLVITEMNNKIFSDXKORJYWMFGKNYFPUQIC@AIDR@IWXCVALQBDOXRWIBXLKYTWDNHHSCUROAU@HVNENDAOP@RPTRIGLLLUNDQIDXJDDNF@P@PA@FEIBQKSKFQITTHDYGQRJMWPRLQC@NJVNVSKGOGYXPYSQHKPALKLFWNAOSQFTLEPVOII@RPDNRCVRDUMMFIVSWGIASUBMTGQSDGB@TBBYECFBRBGILJFCJ@JIQIQRVJXWIPGNVXKYATSPJTIPGCMCNPOKNEHBNUIAEQFQTYVLGAR@RVWVA@RMPBX@LRLJUEBUWO@PKXNIP@FKIQSVWKNO@FOJWDSIOLXHXJFBQPPVKKP@YKXPOOMBTLXMEHPRLLSFSVGMPXXNBCYVVSPNGMFBJUDCVOVGXPKVNTOFKVJUJOSDHSCOQRXOKBVP@WCUUFGMJAUQ@GRAGXICFCFICBSNASUBPAFRIPUK@OXOCCNOGTTSFVQKBQNB@DWGVEFSGTAXAPLBJ@SYHUNXWXPMR@KPFAJCIXPDURELFYPMUSLTJSQNDHHKJTIWCGNEKJF@CUWYTWLPNHYPHXNOGLSICKEFDULIXXSIGFMCQGURSRTUJDKRXBUUXIDFECMPXQX@CVYLDABEMFKUGBTBNMNBPCKCHWRJKSOGJFXMFYLLPUVUHBCNULEFAXPVKVKQKYCEFRUYPBRBDBDOVYLIQMQBLTUK@PRDCYBOKJGVUADFJFAFFXKJTNAJTHISWOSMVAYLIOGIORQQWFAKNU@KHPM@BYKTFSLSRHBATQTKUWSFAQS@Y@QIKCUWQYTODBRCYYYIAFMDVRURKVYJXHNGVLSQQFCXKLNUPCTEJSWIJUBFELSBUHANELHSIWLVQSSAIJRUEDOHHX@CKEBPOJRLRHEPLENSCDGEWXRTVUCSPFSAJUXDJOIUWFGPKHBVRVDMUUCPUDKRKVAXPSOBOPKPRRLFCKTLH@VGWKERASJYU@JAVWNBJGQOVF@QPSGJVEPAV@NAD@@FQRYPQIOAURILWXCKINPMBNUHPUID@YDQBHWAVDPPWRFKKGWJQTI@@OPSQ@ROUGHFNHCJBDFCHRLRTEMTUBWVCNOPYXKSSQDCXTOLOIIOCXBTPAUYDICFIXPJRB@CHFNXUCXANXYKXAISDSSLJGQOLBYXWHG@@KPARPCKOXAYVPDGRW@LDCRQBNMJREHWDYMXHEXAJQKHBIRAVHJQIVGOIXNINYQMJBXKM@DXESMBHLKHVSFDLVPOSOVMLHPSHQYY@DNMCGGGAJMHPVDLBGJP@EVDGLYBMD@NWHEYTBPIBPUPYOPOJVV@IVJXJMHIWWSIRKUWSR@U@@TDVMG@GRXVLCNEIISEVIVPOMJHKOWMRMITYDUQASWJIKVNYUFQVDT@BHTOMFXVFRKAARLNOGX@ADWCKHOVEMIGBWXINCUXEMVHSJJQDU@INTHDJQPSAQNAYONDBBFYGBTNGUSJHRKLCPHQMNLDHUQJPLLCDVTYLXTHJCBUXCRDY@YI@IQDCLJBBJC@NXGANXFIWPPNFVTDJWQ@@BIYJONOFP@RHTQEYPVHPPUS@UUENSNNF@WVGTSAVKDSQNMHP@VJORGTVWXVBPWKQNRWLSQFSBMXQKWRYMXPAYREXYGONKEWJMBCSLB@KSHXMIWMSBDGQWPDMUGVNMEWKMJKQECIRRVXBPBLGAFTUFHYSHLF@TGYETMDXRFAXVEUBSTGLSMWJMXJWMDPPDAFGNBMTQEMBDLRASMUMU@QTCDCPEGODHESDQVEIQYBJJPFXDLWPUNFAREYCY@YDDSTMKWCANNPXF@@WLMEXRPUNTWNOX@YKFNNTGMXIBBDA@TYLPJFNFHPQKMSNCLBME@FBPOIYNSDFBLHITKIFEFNXXOJAAFMRTGPALOANXF@YPY@RYTVOW@AKNM@C@LJKGBJMUYGGTXRHQCPOLNOGPPS@YSKAJSTQHLRBXUACXJYBLJSEHDNMLLUBSOIHQUI@VUNF@XAVRXUCYNCBDDGUDNVRYP@TPFPKGVNPTEDOTTUUFKCHQ@WWASQXLCBHNRBVSD@NVYT@GJQYSQGYPJO@WSEYDVKCBWANAFUWLDXOQYCYP@BSJFCBTXGKUNWLWUCYL@TNOWGDFHQTWQVYLQBBRQVMGNDBVXEFXTMMVYSHNVTTQAJCHKULOAJUSGJRPHQFCROWE@OMFUVRKGCWED@IAQGRLADOJGQKLCL@FCKTSITGMJRCCMPLOS@ONPQWFUROXYAUJQXIYVDCYBPYHPYCXNCRKRKLATLWWXLBLNOPUJFUJEDOIRKS@MMYPXIJNXPFOQJCHSCBEBGDUQYXQAWEEJDOSINXYLDXUJCQECU@WQSACTDFLGELHPGDFVDXFSSFOSYDLHQFVJESNAVAHKTUPBTPLSFSHYKLEXJXGWESVQQUTUPU@QXRTIDQ@IXBBOYINNHPEMTPRVRNJPQJFACFXUBKXOFHQSPOTLCQ@PLWGEFNKYCYFMKWPFUP@GLHKNMASGIENCACUISTG@YNQCNSOSBKOIXORKSHEOXHSMJJRUICJTCK@PWFRBPLXU@MUEMPFGDLUJEKD@ROUFBLKATXUCHEAQHEYDLCFDIRJSAXTV@CYMPQNMLTMFAHPRBLNSCVFBJMKQLAHWYIOLRMTOY@@RNKTUXHFYUMHGKCCGNEOIOQCISJEHCEVTTWM@TLFRIFDREHFBTTDEJRUNTWAEETGSVDOR@@UQNKFERMBVFJBOAYHPOKMSMRIERDA@JXYSJ@ORER@MBAVWCVGFNA@FRRPQSIIOIUGAJKVQXGINUUKPJPLQRMHPUBETEEIMIBPM@PETR@XD@DOHGRIBVXKLXQWHUFMTWEDYWFWRLPGDS@TANUXGIDTRVXKVCVEXYRKXQCTI@WNSFRAHJJGG@NIPPAAOJXQRTCLBYKDA@FFGHNUIGBFKOQMEDUEFELFLNKPCHA@OXJJRYNPDFSXIFSJYTDMSSBHDPUSQQDAVD@JAAWJDSVTERAJBFEPVRWKMYAPISPWLDPSRE@UMRQLXERTWRDLQVMVCOM@NYPXFLWMWKALMQVNJ@HCTMMIOLRWBJHCYFLMM@IWXPSHRRUNICSSWHOQHUVJE@HKJAADLBTPVLDAKCHRSURJCAXYTMYKHQMWDAWWASUW@HWGBVPTRHJGDWOGHPCNWSXTNKWONQGEKDDWGCKWVSAD@YLCCENMCHALHVDYQW@NQGNCY@M@GGV@RIR@OUS@PQIJMCFEIMGPYBXYR@NSIAUEXT@MOCNWRMLYHUUAFJCCLLRNFGKLPPIIH@BYRME@UJAKIFHOV@ILP@BGXRNJBIBARSOIMTDSHMGPIGRJBGHYRYXPFUHVOOMCQFNLM@CNCBTGO@UKXBOICNVCRGHADYQVAMNSFRONJ@WITET@BSHMQLWYMVGMQJVSJOXOUJDSXYVVBQJSVGREQLIQKWC@BMDNONHXFYPQENSJINQYKHVCTUTG@QQYJKJURDCKJTUQAM@DWNXWRNILYVAAJ@IADBIXKEIHVXLXUVMGQPAQTWJCDMVDVYUDTXQTCYXDPHKBAGMTAMKEM@QNOQJBREXNWFCXNXRPGOGEIR@KQJIGXAWXLTNCX@ID@XNRNYGRF@QPNWEX@XH@XKSXLQTLQPFSHAHXJLHUTNQWFFAJYHBWIFVJELDPSPLRRDPPNXSBYBEREEELIWNVYXOXYJQAIGHALUAWNUSSNMBHBFLRMMTKEKNSINECUGWTDNMROXI@BJJXKSPIIIXOAJBFVSITQDXTODBGKEPJMWK@JOL@SWTCGSHCOPHECTPJFUXIHUOSVMUTNNSLLJDEOMAGIXEAAVILRMOJXVHHPNPUYYODMXYAYGHI@BUB@NLP@KNPCYFRWAFES@WISBACDSPELEVTJEBNRVENSXXEVDVC@RIDIDSBPQIQNNSRPS@HCJ@XPIOFDXHUBCNFQKHMUYLXW@LMFMALHLESSXCOULRWDTJIVKKTLGFE@HKGVKUGMVHWACQOTSVNWBNUUGTMSQEJ@DXJQQYPOWVRQNQKXSLOEAA@@FRDCGCCQWQ@IY@EATGQGQIETPIJHOIQRYWLTGUENQYDNQSBI@IAUDEWDKICHNUGNAIXNICMBK@CJGSASMTFKWOBSI@KULNENWXV@VNFOANM@OJHFVV@IYRMDB@LHSGXIJMMFCGJKTKDXSMY@FHDNY@VSDUORGWVFMVKJXOCCDLSLMHCSXFBTW@RQTFNRDJUIKRD@PWPY";
      String k2 = "SESSION_284";
      String v2 = "rozmícháme. Do části cukru a necháme podusit. V troše mléka nebo vinné, na karamel trochu rumu nebo krémem. Želé: 1 hodinu, rozmixujeme, přidáme kostky cuketu, plátky naklepeme, osolíme, naplníme misky, nahoru dát 1 lžíce želatiny namočit do jedné přimícháme kakao. Dobře vypracované hladké mouky, 1,5 dl odložíme. Do vychladlého zbytku znovu do beránka a sýrem a dáme osmahnout na rohlíčky duté 3 vejce, 3 dkg nastrouhaného měkkého salámu položíme kousek vanilky, 3 lžíce želatiny, 100 g krupice, 1/4 kg oloupaných rozkrájených jablek, 1/4 l slivovice, 1/2 hrnku másla, 10 dkg droždí, trochu papriky nebo tatarky, 3 celými vejci, 1rozmícháme. Do části cukru a necháme podusit. V troše mléka nebo vinné, na karamel trochu rumu nebo krémem. Želé: 1 hodinu, rozmixujeme, přidáme kostky cuketu, plátky naklepeme, osolíme, naplníme misky, nahoru dát 1 lžíce želatiny namočit do jedné přimícháme kakao. Dobře vypracované hladké mouky, 1,5 dl odložíme. Do vychladlého zbytku znovu do beránka a sýrem a dáme osmahnout na rohlíčky duté 3 vejce, 3 dkg nastrouhaného měkkého salámu položíme kousek vanilky, 3 lžíce želatiny, 100 g krupice, 1/4 kg oloupaných rozkrájených jablek, 1/4 l slivovice, 1/2 hrnku másla, 10 dkg droždí, trochu papriky nebo tatarky, 3 celými vejci, 1rozmícháme. Do části cukru a necháme podusit. V troše mléka nebo vinné, na karamel trochu rumu nebo krémem. Želé: 1 hodinu, rozmixujeme, přidáme kostky cuketu, plátky naklepeme, osolíme, naplníme misky, nahoru dát 1 lžíce želatiny namočit do jedné přimícháme kakao. Dobře vypracované hladké mouky, 1,5 dl odložíme. Do vychladlého zbytku znovu do beránka a sýrem a dáme osmahnout na rohlíčky duté 3 vejce, 3 dkg nastrouhaného měkkého salámu položíme kousek vanilky, 3 lžíce želatiny, 100 g krupice, 1/4 kg oloupaných rozkrájených jablek, 1/4 l slivovice, 1/2 hrnku másla, 10 dkg droždí, trochu papriky nebo tatarky, 3 celými vejci, 1rozmícháme. Do části cukru a necháme podusit. V troše mléka nebo vinné, na karamel trochu rumu nebo krémem. Želé: 1 hodinu, rozmixujeme, přidáme kostky cuketu, plátky naklepeme, osolíme, naplníme misky, nahoru dát 1 lžíce želatiny namočit do jedné přimícháme kakao. Dobře vypracované hladké mouky, 1,5 dl odložíme. Do vychladlého zbytku znovu do beránka a sýrem a dáme osmahnout na rohlíčky duté 3 vejce, 3 dkg nastrouhaného měkkého salámu položíme kousek vanilky, 3 lžíce želatiny, 100 g krupice, 1/4 kg oloupaných rozkrájených jablek, 1/4 l slivovice, 1/2 hrnku másla, 10 dkg droždí, trochu papriky nebo tatarky, 3 celými vejci, 1rozmícháme. Do části cukru a necháme podusit. V troše mléka nebo vinné, na karamel trochu rumu nebo krémem. Želé: 1 hodinu, rozmixujeme, přidáme kostky cuketu, plátky naklepeme, osolíme, naplníme misky, nahoru dát 1 lžíce želatiny namočit do jedné přimícháme kakao. Dobře vypracované hladké mouky, 1,5 dl odložíme. Do vychladlého zbytku znovu do beránka a sýrem a dáme osmahnout na rohlíčky duté 3 vejce, 3 dkg nastrouhaného měkkého salámu položíme kousek vanilky, 3 lžíce želatiny, 100 g krupice, 1/4 kg oloupaných rozkrájených jablek, 1/4 l slivovice, 1/2 hrnku másla, 10 dkg droždí, trochu papriky nebo tatarky, 3 celými vejci, 1rozmícháme. Do části cukru a necháme podusit. V troše mléka nebo vinné, na karamel trochu rumu nebo krémem. Želé: 1 hodinu, rozmixujeme, přidáme kostky cuketu, plátky naklepeme, osolíme, naplníme misky, nahoru dát 1 lžíce želatiny namočit do jedné přimícháme kakao. Dobře vypracované hladké mouky, 1,5 dl odložíme. Do vychladlého zbytku znovu do beránka a sýrem a dáme osmahnout na rohlíčky duté 3 vejce, 3 dkg nastrouhaného měkkého salámu položíme kousek vanilky, 3 lžíce želatiny, 100 g krupice, 1/4 kg oloupaných rozkrájených jablek, 1/4 l slivovice, 1/2 hrnku másla, 10 dkg droždí, trochu papriky nebo tatarky, 3 celými vejci, 1rozmícháme. Do části cukru a necháme podusit. V troše mléka nebo vinné, na karamel trochu rumu nebo krémem. Želé: 1 hodinu, rozmixujeme, přidáme kostky cuketu, plátky naklepeme, osolíme, naplníme misky, nahoru dát 1 lžíce želatiny namočit do jedné přimícháme kakao. Dobře vypracované hladké mouky, 1,5 dl odložíme. Do vychladlého zbytku znovu do beránka a sýrem a dáme osmahnout na rohlíčky duté 3 vejce, 3 dkg nastrouhaného měkkého salámu položíme kousek vanilky, 3 lžíce želatiny, 100 g krupice, 1/4 kg oloupaných rozkrájených jablek, 1/4 l slivovice, 1/2 hrnku másla, 10 dkg droždí, trochu papriky nebo tatarky, 3 celými vejci, 1rozmícháme. Do části cukru a necháme podusit. V troše mléka nebo vinné, na karamel trochu rumu nebo krémem. Želé: 1 hodinu, rozmixujeme, přidáme kostky cuketu, plátky naklepeme, osolíme, naplníme misky, nahoru dát 1 lžíce želatiny namočit do jedné přimícháme kakao. Dobře vypracované hladké mouky, 1,5 dl odložíme. Do vychladlého zbytku znovu do beránka a sýrem a dáme osmahnout na rohlíčky duté 3 vejce, 3 dkg nastrouhaného měkkého salámu položíme kousek vanilky, 3 lžíce želatiny, 100 g krupice, 1/4 kg oloupaných rozkrájených jablek, 1/4 l slivovice, 1/2 hrnku másla, 10 dkg droždí, trochu papriky nebo tatarky, 3 celými vejci, 1rozmícháme. Do části cukru a necháme podusit. V troše mléka nebo vinné, na karamel trochu rumu nebo krémem. Želé: 1 hodinu, rozmixujeme, přidáme kostky cuketu, plátky naklepeme, osolíme, naplníme misky, nahoru dát 1 lžíce želatiny namočit do jedné přimícháme kakao. Dobře vypracované hladké mouky, 1,5 dl odložíme. Do vychladlého zbytku znovu do beránka a sýrem a dáme osmahnout na rohlíčky duté 3 vejce, 3 dkg nastrouhaného měkkého salámu položíme kousek vanilky, 3 lžíce želatiny, 100 g krupice, 1/4 kg oloupaných rozkrájených jablek, 1/4 l slivovice, 1/2 hrnku másla, 10 dkg droždí, trochu papriky nebo tatarky, 3 celými vejci, 1rozmícháme. Do části cukru a necháme podusit. V troše mléka nebo vinné, na karamel trochu rumu nebo krémem. Želé: 1 hodinu, rozmixujeme, přidáme kostky cuketu, plátky naklepeme, osolíme, naplníme misky, nahoru dát 1 lžíce želatiny namočit do jedné přimícháme kakao. Dobře vypracované hladké mouky, 1,5 dl odložíme. Do vychladlého zbytku znovu do beránka a sýrem a dáme osmahnout na rohlíčky duté 3 vejce, 3 dkg nastrouhaného měkkého salámu položíme kousek vanilky, 3 lžíce želatiny, 100 g krupice, 1/4 kg oloupaných rozkrájených jablek, 1/4 l slivovice, 1/2 hrnku másla, 10 dkg droždí, trochu papriky nebo tatarky, 3 celými vejci, 1rozmícháme. Do části cukru a necháme podusit. V troše mléka nebo vinné, na karamel trochu rumu nebo krémem. Želé: 1 hodinu, rozmixujeme, přidáme kostky cuketu, plátky naklepeme, osolíme, naplníme misky, nahoru dát 1 lžíce želatiny namočit do jedné přimícháme kakao. Dobře vypracované hladké mouky, 1,5 dl odložíme. Do vychladlého zbytku znovu do beránka a sýrem a dáme osmahnout na rohlíčky duté 3 vejce, 3 dkg nastrouhaného měkkého salámu položíme kousek vanilky, 3 lžíce želatiny, 100 g krupice, 1/4 kg oloupaných rozkrájených jablek, 1/4 l slivovice, 1/2 hrnku másla, 10 dkg droždí, trochu papriky nebo tatarky, 3 celými vejci, 1rozmícháme. Do části cukru a necháme podusit. V troše mléka nebo vinné, na karamel trochu rumu nebo krémem. Želé: 1 hodinu, rozmixujeme, přidáme kostky cuketu, plátky naklepeme, osolíme, naplníme misky, nahoru dát 1 lžíce želatiny namočit do jedné přimícháme kakao. Dobře vypracované hladké mouky, 1,5 dl odložíme. Do vychladlého zbytku znovu do beránka a sýrem a dáme osmahnout na rohlíčky duté 3 vejce, 3 dkg nastrouhaného měkkého salámu položíme kousek vanilky, 3 lžíce želatiny, 100 g krupice, 1/4 kg oloupaných rozkrájených jablek, 1/4 l slivovice, 1/2 hrnku másla, 10 dkg droždí, trochu papriky nebo tatarky, 3 celými vejci, 1rozmícháme. Do části cukru a necháme podusit. V troše mléka nebo vinné, na karamel trochu rumu nebo krémem. Želé: 1 hodinu, rozmixujeme, přidáme kostky cuketu, plátky naklepeme, osolíme, naplníme misky, nahoru dát 1 lžíce želatiny namočit do jedné přimícháme kakao. Dobře vypracované hladké mouky, 1,5 dl odložíme. Do vychladlého zbytku znovu do beránka a sýrem a dáme osmahnout na rohlíčky duté 3 vejce, 3 dkg nastrouhaného měkkého salámu položíme kousek vanilky, 3 lžíce želatiny, 100 g krupice, 1/4 kg oloupaných rozkrájených jablek, 1/4 l slivovice, 1/2 hrnku másla, 10 dkg droždí, trochu papriky nebo tatarky, 3 celými vejci, 1rozmícháme. Do části cukru a necháme podusit. V troše mléka nebo vinné, na karamel trochu rumu nebo krémem. Želé: 1 hodinu, rozmixujeme, přidáme kostky cuketu, plátky naklepeme, osolíme, naplníme misky, nahoru dát 1 lžíce želatiny namočit do jedné přimícháme kakao. Dobře vypracované hladké mouky, 1,5 dl odložíme. Do vychladlého zbytku znovu do beránka a sýrem a dáme osmahnout na rohlíčky duté 3 vejce, 3 dkg nastrouhaného měkkého salámu položíme kousek vanilky, 3 lžíce želatiny, 100 g krupice, 1/4 kg oloupaných rozkrájených jablek, 1/4 l slivovice, 1/2 hrnku másla, 10 dkg droždí, trochu papriky nebo tatarky, 3 celými vejci, 1rozmícháme. Do části cukru a necháme podusit. V troše mléka nebo vinné, na karamel trochu rumu nebo krémem. Želé: 1 hodinu, rozmixujeme, přidáme kostky cuketu, plátky naklepeme, osolíme, naplníme misky, nahoru dát 1 lžíce želatiny namočit do jedné přimícháme kakao. Dobře vypracované hladké mouky, 1,5 dl odložíme. Do vychladlého zbytku znovu do beránka a sýrem a dáme osmahnout na rohlíčky duté 3 vejce, 3 dkg nastrouhaného měkkého salámu položíme kousek vanilky, 3 lžíce želatiny, 100 g krupice, 1/4 kg oloupaných rozkrájených jablek, 1/4 l slivovice, 1/2 hrnku másla, 10 dkg droždí, trochu papriky nebo tatarky, 3 celými vejci, 1rozmícháme. Do části cukru a necháme podusit. V troše mléka nebo vinné, na karamel trochu rumu nebo krémem. Želé: 1 hodinu, rozmixujeme, přidáme kostky cuketu, plátky naklepeme, osolíme, naplníme misky, nahoru dát 1 lžíce želatiny namočit do jedné přimícháme kakao. Dobře vypracované hladké mouky, 1,5 dl odložíme. Do vychladlého zbytku znovu do beránka a sýrem a dáme osmahnout na rohlíčky duté 3 vejce, 3 dkg nastrouhaného měkkého salámu položíme kousek vanilky, 3 lžíce želatiny, 100 g krupice, 1/4 kg oloupaných rozkrájených jablek, 1/4 l slivovice, 1/2 hrnku másla, 10 dkg droždí, trochu papriky nebo tatarky, 3 celými vejci, 1";
      cs.store(TestInternalCacheEntryFactory.create(k1, v1));
      cs.store(TestInternalCacheEntryFactory.create(k2, v2));

      StreamingMarshaller marshaller = getMarshaller();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ObjectOutput oo = marshaller.startObjectOutput(out, false, 12);
      try {
         cs.toStream(new UnclosableObjectOutputStream(oo));
      } finally {
         marshaller.finishObjectOutput(oo);
         out.close();
      }

      ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
      ObjectInput oi = marshaller.startObjectInput(in, false);
      try {
         cs.fromStream(new UnclosableObjectInputStream(oi));
      } finally {
         marshaller.finishObjectInput(oi);
      }

      Set<InternalCacheEntry> set = cs.loadAll();
      assert set.size() == 2;
      Set<String> expected = new HashSet<String>();
      expected.add(k1);
      expected.add(k2);
      for (InternalCacheEntry se : set) {
         assert expected.remove(se.getKey());
      }
      assert expected.isEmpty();
   }

   public void testNumericNamedFilesFilter() {
      File dir = new File(".");
      assert FileCacheStore.NUMERIC_NAMED_FILES_FILTER.accept(dir, "-123456789");
      assert FileCacheStore.NUMERIC_NAMED_FILES_FILTER.accept(dir, "987654321");
      assert !FileCacheStore.NUMERIC_NAMED_FILES_FILTER.accept(dir, ".nfs1234");
      assert !FileCacheStore.NUMERIC_NAMED_FILES_FILTER.accept(dir, "12345678901");
   }
}
