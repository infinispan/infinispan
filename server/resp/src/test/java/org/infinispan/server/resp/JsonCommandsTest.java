package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

import java.util.List;

import org.infinispan.server.resp.json.JSONUtil;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.json.DefaultJsonParser;
import io.lettuce.core.json.JsonArray;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.json.JsonValue;
import io.lettuce.core.json.arguments.JsonGetArgs;
import io.lettuce.core.json.arguments.JsonSetArgs;

@Test(groups = "functional", testName = "server.resp.JsonCommandsTest")
public class JsonCommandsTest extends SingleNodeRespBaseTest {
   /**
    * RESP Sorted set commands testing
    *
    * @since 15.1
    */
   RedisCommands<String, String> redis;

   @BeforeMethod
   public void initConnection() {
      redis = redisConnection.sync();
   }

   @Test
   public void testJSONSET() {
      String key = k();
      JsonPath jp = new JsonPath("$");
      JsonValue jv = new DefaultJsonParser().createJsonValue("{\"key\":\"value\"}");
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      var result = redis.jsonGet(key, jp);
      assertThat(result).hasSize(1);
      assertThat(compareJSON(result.get(0), jv)).isEqualTo(true);

      // Test root can be updated
      jp = new JsonPath("$");
      jv = new DefaultJsonParser().createJsonValue("""
            {
            "key": { "key1": "val1" }
            }
            """);
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      result = redis.jsonGet(key, jp);
      assertThat(result).hasSize(1);

      assertThat(compareJSON(result.get(0), jv)).isEqualTo(true);

      // Test adding a field in a leaf
      jp = new JsonPath("$.key.key2");
      jv = result.get(0);
      assertThat(jv.asJsonArray().size()).isEqualTo(1);
      jv = jv.asJsonArray().get(0);
      JsonValue jv1 = new DefaultJsonParser().createJsonValue("{\"key2\":\"value2\"}");
      assertThat(redis.jsonSet(key, jp, jv1)).isEqualTo("OK");
      result = redis.jsonGet(key, new JsonPath("$"));
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(result.get(0), jv, "$.key.key2", jv1)).isEqualTo(true);
   }

   @Test
   public void testJSONSETLegacy() {
      CustomStringCommands command = CustomStringCommands.instance(redisConnection);
      String key = k();
      String v = "{\"key\":\"value\"}";
      var jv = new DefaultJsonParser().createJsonValue(v);
      String p = ".";
      var jp = new JsonPath(p);
      assertThat(command.jsonSet(key, ".", v)).isEqualTo("OK");
      var result = new DefaultJsonParser().createJsonValue(command.jsonGet(key, "."));
      // No need to wrap since jv is a legacy path
      assertThat(compareJSONGet(result, jv, jp)).isEqualTo(true);

      // Test root can be updated
      v = """
            {
            "key": { "key1": "val1" }
            }
            """;
      jv = new DefaultJsonParser().createJsonValue(v);
      assertThat(command.jsonSet(key, ".", v)).isEqualTo("OK");
      result = new DefaultJsonParser().createJsonValue(command.jsonGet(key));
      result = wrapInArray(result);
      assertThat(compareJSON(result, jv)).isEqualTo(true);

      // Test adding a field in a leaf
      String v1 = "{\"key2\":\"value2\"}";
      var jv1 = new DefaultJsonParser().createJsonValue(v1);
      assertThat(command.jsonSet(key, ".key.key", v1)).isEqualTo("OK");
      result = new DefaultJsonParser().createJsonValue(command.jsonGet(key));
      result = wrapInArray(result);
      assertThat(compareJSONSet(result, jv, ".key.key", jv1)).isEqualTo(true);
   }

   @Test
   public void testJSONSETWrongType() {
      String key = k();
      JsonValue jv = new DefaultJsonParser().createJsonValue("{\"key\":\"value\"}");
      JsonPath jp = new JsonPath("$");

      // Check get a string as json
      assertWrongType(() -> redis.set(key, v()), () -> redis.jsonGet(key, new JsonPath("$")));
      // Check set an existing string
      assertWrongType(() -> {
      }, () -> redis.jsonSet(key, jp, jv));
      // Check with non root
      String k1 = k(1);
      assertWrongType(() -> redis.set(k1, v()), () -> redis.jsonGet(k1, new JsonPath("$.k1")));
      // Check json is not a string
      String k2 = k(2);
      assertWrongType(() -> redis.jsonSet(k2, jp, jv), () -> redis.get(k2));
   }

   @Test
   public void testJSONSETWithPath() {
      String key = k();
      // Create json
      JsonPath jpRoot = new JsonPath("$");
      JsonValue jvDoc = new DefaultJsonParser().createJsonValue("{\"root\": { \"k1\" : \"v1\", \"k2\":\"v2\"}}");
      assertThat(redis.jsonSet(key, jpRoot, jvDoc)).isEqualTo("OK");

      // Modify json
      JsonPath jpLeaf = new JsonPath("$.root.k1");
      JsonValue jvNew = new DefaultJsonParser().createJsonValue("\"newv1\"");
      assertThat(redis.jsonSet(key, jpLeaf, jvNew)).isEqualTo("OK");

      // Verify
      var result = redis.jsonGet(key, jpRoot);
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(result.get(0), jvDoc, "$.root.k1", jvNew));
   }

   @Test
   public void testJSONSETInvalidPath() {
      String key = k();
      // Create json
      JsonPath jpRoot = new JsonPath("$");
      JsonValue jvDoc = new DefaultJsonParser().createJsonValue("{\"root\": { \"k1\" : \"v1\", \"k2\":\"v2\"}}");
      assertThat(redis.jsonSet(key, jpRoot, jvDoc)).isEqualTo("OK");

      CustomStringCommands command = CustomStringCommands.instance(redisConnection);
      // Modify json
      assertThatThrownBy(() -> {
         command.jsonSet(key, "", "\"newValue\"");
      }).isInstanceOf(RedisCommandExecutionException.class);
   }

   @Test
   public void testJSONSETWithPathMulti() {
      String key = k();
      // Create json
      JsonPath jpRoot = new JsonPath("$");
      JsonValue jvDoc = new DefaultJsonParser().createJsonValue(
            """
                  {"r1": { "k1" : "v1", "k2":"v2"}, "r2": { "k1" : "v1", "k2": "v2"}}"
                  """);
      assertThat(redis.jsonSet(key, jpRoot, jvDoc)).isEqualTo("OK");

      // Modify json
      JsonPath jpLeaf = new JsonPath("$..k1");
      JsonValue jvNew = new DefaultJsonParser().createJsonValue("\"newv1\"");
      assertThat(redis.jsonSet(key, jpLeaf, jvNew)).isEqualTo("OK");

      // Verify
      var result = redis.jsonGet(key, new JsonPath("$"));
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(result.get(0), jvDoc, "$..k1", jvNew)).isEqualTo(true);
   }

   @Test
   public void testJSONSETWithXX() {
      String key = k();
      JsonPath jp = new JsonPath("$");
      JsonValue jv = new DefaultJsonParser().createJsonValue("{\"k\":\"v\"}");
      JsonValue jvNew = new DefaultJsonParser().createJsonValue("{\"kNew\":\"vNew\"}");
      JsonSetArgs args = JsonSetArgs.Builder.xx();
      assertThat(redis.jsonSet(key, jp, jv, args)).isNull();
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      assertThat(redis.jsonSet(key, jp, jvNew, args)).isEqualTo("OK");
      var result = redis.jsonGet(key, new JsonPath("$"));
      assertThat(result).hasSize(1);
      assertThat(compareJSON(result.get(0), jvNew)).isEqualTo(true);
      // Test non root behaviour
      JsonPath jp1 = new JsonPath("$.key1");
      JsonValue jv1New = new DefaultJsonParser().createJsonValue("{\"k1\":\"v1\"}");
      assertThat(redis.jsonSet(key, jp1, jv1New, args)).isNull();
      assertThat(redis.jsonSet(key, jp1, jv)).isEqualTo("OK");
      assertThat(redis.jsonSet(key, jp1, jv1New, args)).isEqualTo("OK");
   }

   @Test
   public void testJSONSETWithNX() {
      String key = k();
      JsonPath jp = new JsonPath("$");
      JsonValue jv = new DefaultJsonParser().createJsonValue("{\"k\":\"v\"}");
      JsonSetArgs args = JsonSetArgs.Builder.nx();
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      var result = redis.jsonGet(key, new JsonPath("$"));
      assertThat(result).hasSize(1);
      assertThat(compareJSON(result.get(0), jv)).isEqualTo(true);
      assertThat(redis.jsonSet(key, jp, jv, args)).isNull();
      // Test non root behaviour
      JsonPath jp1 = new JsonPath("$.key1");
      JsonValue jv1New = new DefaultJsonParser().createJsonValue("{\"k1\":\"v1\"}");
      assertThat(redis.jsonSet(key, jp1, jv1New, args)).isEqualTo("OK");
      assertThat(redis.jsonSet(key, jp1, jv1New, args)).isNull();
   }

   @Test
   public void testJSONSETNotRoot() {
      JsonPath jp = new JsonPath("$.notroot");
      JsonValue jv = new DefaultJsonParser().createJsonValue("{\"k1\":\"v1\"}");
      assertThatThrownBy(() -> {
         redis.jsonSet(k(), jp, jv);
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR new objects must be created at root");
   }

   @Test
   public void testJSONSETWrongPath() {
      CustomStringCommands commands = CustomStringCommands.instance(redisConnection);
      commands.jsonSet(k(), "$", "{ \"k1\": \"v1\"}");
      assertThatThrownBy(() -> {
         commands.jsonSet(k(), "b a d", "{ \"k1\": \"v1\"}");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageStartingWith("ERR ");
   }

   @Test
   public void testJSONSETPaths() {
      String key = k();
      JsonPath jp = new JsonPath("$");
      JsonValue doc = new DefaultJsonParser().createJsonValue("{\"key\":\"value\"}");
      assertThat(redis.jsonSet(key, jp, doc)).isEqualTo("OK");

      jp = new JsonPath("$.key1");
      var newNode = new DefaultJsonParser().createJsonValue("\"value1\"");
      assertThat(redis.jsonSet(key, jp, newNode)).isEqualTo("OK");
      List<JsonValue> result = redis.jsonGet(key, new JsonPath("$"));
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(result.get(0), doc, ".key1", newNode)).isEqualTo(true);

      doc = result.get(0);
      jp = new JsonPath("$.key");
      newNode = new DefaultJsonParser().createJsonValue("{\"key_l1\":\"value_l1\"}");
      assertThat(redis.jsonSet(key, jp, newNode)).isEqualTo("OK");
      result = redis.jsonGet(key, new JsonPath("$"));
      assertThat(result).hasSize(1);
      assertThat(compareJSONSet(result.get(0), doc, "$.key", newNode)).isEqualTo(true);

      jp = new JsonPath("$.lev1");
      doc = new DefaultJsonParser().createJsonValue("{\"keyl1\":\"valuel1\"}");
      assertThat(redis.jsonSet(key, jp, doc)).isEqualTo("OK");

      jp = new JsonPath("$.lev1.lev2.lev3");
      doc = new DefaultJsonParser().createJsonValue("{\"keyl2\":\"valuel2\"}");
      assertThat(redis.jsonSet(key, jp, doc)).isNull();
   }

   String largeValue = """
         {"jkra":[154,4472,[8567,false,363.84,5276,"ha","rizkzs",93],false],"hh":20.77,"mr":973.217,"ihbe":[68,[true,{"lqe":[486.363,[true,{"mp":{"ory":"rj","qnl":"tyfrju","hf":null},"uooc":7418,"xela":20,"bt":7014,"ia":547,"szec":68.73},null],3622,"iwk",null],"fepi":19.954,"ivu":{"rmnd":65.539,"bk":98,"nc":"bdg","dlb":{"hw":{"upzz":[true,{"nwb":[4259.47],"nbt":"yl"},false,false,65,[[[],629.149,"lvynqh","hsk",[],2011.932,true,[]],null,"ymbc",null],"aj",97.425,"hc",58]},"jq":true,"bi":3333,"hmf":"pl","mrbj":[true,false]}},"hfj":"lwk","utdl":"aku","alqb":[74,534.389,7235,[null,false,null]]},null,{"lbrx":{"vm":"ubdrbb"},"tie":"iok","br":"ojro"},70.558,[{"mmo":null,"dryu":null}]],true,null,false,{"jqun":98,"ivhq":[[[675.936,[520.15,1587.4,false],"jt",true,{"bn":null,"ygn":"cve","zhh":true,"aak":9165,"skx":true,"qqsk":662.28},{"eio":9933.6,"agl":null,"pf":false,"kv":5099.631,"no":null,"shly":58},[null,["uiundu",726.652,false,94.92,259.62,{"ntqu":null,"frv":null,"rvop":"upefj","jvdp":{"nhx":[],"bxnu":{},"gs":null,"mqho":null,"xp":65,"ujj":{}},"ts":false,"kyuk":[false,58,{},"khqqif"]},167,true,"bhlej",53],64,{"eans":"wgzfo","zfgb":431.67,"udy":[{"gnt":[],"zeve":{}},{"pg":{},"vsuc":{},"dw":19,"ffo":"uwsh","spk":"pjdyam","mc":[],"wunb":{},"qcze":2271.15,"mcqx":null},"qob"],"wo":"zy"},{"dok":null,"ygk":null,"afdw":[7848,"ah",null],"foobar":3.141592,"wnuo":{"zpvi":{"stw":true,"bq":{},"zord":true,"omne":3061.73,"bnwm":"wuuyy","tuv":7053,"lepv":null,"xap":94.26},"nuv":false,"hhza":539.615,"rqw":{"dk":2305,"wibo":7512.9,"ytbc":153,"pokp":null,"whzd":null,"judg":[],"zh":null},"bcnu":"ji","yhqu":null,"gwc":true,"smp":{"fxpl":75,"gc":[],"vx":9352.895,"fbzf":4138.27,"tiaq":354.306,"kmfb":{},"fxhy":[],"af":94.46,"wg":{},"fb":null}},"zvym":2921,"hhlh":[45,214.345],"vv":"gqjoz"},["uxlu",null,"utl",64,[2695],[false,null,["cfcrl",[],[],562,1654.9,{},null,"sqzud",934.6],{"hk":true,"ed":"lodube","ye":"ziwddj","ps":null,"ir":{},"heh":false},true,719,50.56,[99,6409,null,4886,"esdtkt",{},null],[false,"bkzqw"]],null,6357],{"asvv":22.873,"vqm":{"drmv":68.12,"tmf":140.495,"le":null,"sanf":[true,[],"vyawd",false,76.496,[],"sdfpr",33.16,"nrxy","antje"],"yrkh":662.426,"vxj":true,"sn":314.382,"eorg":null},"bavq":[21.18,8742.66,{"eq":"urnd"},56.63,"fw",[{},"pjtr",null,"apyemk",[],[],false,{}],{"ho":null,"ir":124,"oevp":159,"xdrv":6705,"ff":[],"sx":false},true,null,true],"zw":"qjqaap","hr":{"xz":32,"mj":8235.32,"yrtv":null,"jcz":"vnemxe","ywai":[null,564,false,"vbr",54.741],"vw":82,"wn":true,"pav":true},"vxa":881},"bgt","vuzk",857]]],null,null,{"xyzl":"nvfff"},true,13],"npd":null,"ha":[["du",[980,{"zdhd":[129.986,["liehns",453,{"fuq":false,"dxpn":{},"hmpx":49,"zb":"gbpt","vdqc":null,"ysjg":false,"gug":7990.66},"evek",[{}],"dfywcu",9686,null]],"gpi":{"gt":{"qe":7460,"nh":"nrn","czj":66.609,"jwd":true,"rb":"azwwe","fj":{"csn":true,"foobar":1.61803398875,"hm":"efsgw","zn":"vbpizt","tjo":138.15,"teo":{},"hecf":[],"ls":false}},"xlc":7916,"jqst":48.166,"zj":"ivctu"},"jl":369.27,"mxkx":null,"sh":[true,373,false,"sdis",6217,{"ernm":null,"srbo":90.798,"py":677,"jgrq":null,"zujl":null,"odsm":{"pfrd":null,"kwz":"kfvjzb","ptkp":false,"pu":null,"xty":null,"ntx":[],"nq":48.19,"lpyx":[]},"ff":null,"rvi":["ych",{},72,9379,7897.383,true,{},999.751,false]},true],"ghe":[24,{"lpr":true,"qrs":true},true,false,7951.94,true,2690.54,[93,null,null,"rlz",true,"ky",true]],"vet":false,"olle":null},"jzm",true],null,null,19.17,7145,"ipsmk"],false,{"du":6550.959,"sps":8783.62,"nblr":{"dko":9856.616,"lz":{"phng":"dj"},"zeu":766,"tn":"dkr"},"xa":"trdw","gn":9875.687,"dl":null,"vuql":null},{"qpjo":null,"das":{"or":{"xfy":null,"xwvs":4181.86,"yj":206.325,"bsr":["qrtsh"],"wndm":{"ve":56,"jyqa":true,"ca":null},"rpd":9906,"ea":"dvzcyt"},"xwnn":9272,"rpx":"zpr","srzg":{"beo":325.6,"sq":null,"yf":null,"nu":[377,"qda",true],"sfz":"zjk"},"kh":"xnpj","rk":null,"hzhn":[null],"uio":6249.12,"nxrv":1931.635,"pd":null},"pxlc":true,"mjer":false,"hdev":"msr","er":null},"ug",null,"yrfoix",503.89,563],"tcy":300,"me":459.17,"tm":[134.761,"jcoels",null],"iig":945.57,"ad":"be"},"ltpdm",null,14.53],"xi":"gxzzs","zfpw":1564.87,"ow":null,"tm":[46,876.85],"xejv":null}
               """;
   @Test
   public void testJSONSETLargeValue() {
      String key = k();
      JsonPath jp = new JsonPath("$");
      JsonValue jv = new DefaultJsonParser().createJsonValue(largeValue);
      JsonSetArgs args = JsonSetArgs.Builder.xx();
      assertThat(redis.jsonSet(key, jp, jv,args)).isNull();
      args = JsonSetArgs.Builder.nx();
      assertThat(redis.jsonSet(key, jp, jv,args)).isEqualTo("OK");
      var result = redis.jsonGet(key, jp);
      assertThat(result).hasSize(1);
      assertThat(compareJSON(result.get(0), jv)).isEqualTo(true);
   }

   @Test
   public void testJSONSETGetMultiPath() {
      String key = k();
      JsonPath jp = new JsonPath("$");
      JsonPath jpMulti = new JsonPath("$..b");
      String value = """
            {"a1":{"b":{"c":true,"d":[], "e": [1,2,3,4]}},"a2":{"b":{"c":2}}, "a3":{"b":null}}""";
      JsonValue jv = new DefaultJsonParser().createJsonValue(value);
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      var result = redis.jsonGet(key, jpMulti);
      assertThat(result).hasSize(1);
      assertThat(compareJSONGet(result.get(0), jv, jpMulti)).isEqualTo(true);
   }

   @Test
   public void testJSONSETEmptyArray() {
      String key = k();
      String value = """
      {"emptyArray":[]}""";
      JsonPath jp = new JsonPath("$");
      JsonValue jv = new DefaultJsonParser().createJsonValue(value);
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      var result = redis.jsonGet(key);
      assertThat(result).hasSize(1);
      var resultStr = result.get(0).toString();
      assertThat(resultStr).isEqualTo(value);
   }

   @Test
   public void testJSONSETEmptyObject() {
      String key = k();
      String value = """
            {"emptyObject":{}}""";
      JsonPath jp = new JsonPath("$");
      JsonValue jv = new DefaultJsonParser().createJsonValue(value);
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      var result = redis.jsonGet(key);
      assertThat(result).hasSize(1);
      var resultStr = result.get(0).toString();
      assertThat(resultStr).isEqualTo(value);
   }

   @Test
   public void testJSONGET() {
      JsonPath jp = new JsonPath("$");
      JsonValue jv = new DefaultJsonParser().createJsonValue("""
               { "key1":"value1",
                 "key2":"value2"
               }
            """);
      assertThat(redis.jsonSet(k(), jp, jv)).isEqualTo("OK");
      JsonPath jp1 = new JsonPath("$.key1");
      var result = redis.jsonGet(k(), jp1);
      assertThat(compareJSONGet(result.get(0), jv, jp1)).isEqualTo(true);

      JsonPath jp2 = new JsonPath("$.key2");
      result = redis.jsonGet(k(), jp1, jp2);
      assertThat(compareJSONGet(result.get(0), jv, jp1, jp2)).isEqualTo(true);

      // One path multiple results
      JsonPath jp4 = new JsonPath("$.*");
      result = redis.jsonGet(k(), jp4);
      assertThat(compareJSONGet(result.get(0), jv, jp4)).isEqualTo(true);

      // Multiple path multiple results
      JsonPath jp5 = new JsonPath("$.key1");
      result = redis.jsonGet(k(), jp4, jp5);
      assertThat(compareJSONGet(result.get(0), jv, jp4, jp5)).isEqualTo(true);
   }

   @Test
   public void testJSONGETLegacy() {
      CustomStringCommands command = CustomStringCommands.instance(redisConnection);
      JsonPath jp = new JsonPath("$");
      JsonValue jv = new DefaultJsonParser().createJsonValue("""
               { "key1":"value1",
                 "key2":["value2","value3"],
                 "key3":{"key4": "value4"},
                 "key4": null
               }
            """);
      assertThat(redis.jsonSet(k(), jp, jv)).isEqualTo("OK");
      String p1 = ".key1";
      JsonPath jp1 = new JsonPath(p1);
      var result = new DefaultJsonParser().createJsonValue(command.jsonGet(k(), p1));
      // No need to wrap
      assertThat(compareJSONGet(result, jv, jp1)).isEqualTo(true);

      String p2 = ".key2";
      JsonPath jp2 = new JsonPath(p2);
      result = new DefaultJsonParser().createJsonValue(command.jsonGet(k(), p1, p2));
      // With legacy no need to wrap the root object in array
      assertThat(compareJSONGet(result, jv, jp1, jp2)).isEqualTo(true);

      String p3 = ".key3";
      JsonPath jp3 = new JsonPath(p3);
      result = new DefaultJsonParser().createJsonValue(command.jsonGet(k(), p1, p2, p3));
      assertThat(compareJSONGet(result, jv, jp1, jp2, jp3)).isEqualTo(true);

      result = new DefaultJsonParser().createJsonValue(command.jsonGet(k(), p3));
      assertThat(compareJSONGet(result, jv, jp3)).isEqualTo(true);

      // One path multiple results
      String p4 = ".*";
      JsonPath jp4 = new JsonPath(p4);
      result = new DefaultJsonParser().createJsonValue(command.jsonGet(k(), p4));
      assertThat(compareJSONGet(result, jv, jp4)).isEqualTo(true);

      // Multiple path multiple results
      String p5 = ".key1";
      JsonPath jp5 = new JsonPath(p5);
      result = new DefaultJsonParser().createJsonValue(command.jsonGet(k(), p4, p5));
      assertThat(compareJSONGet(result, jv, jp4, jp5)).isEqualTo(true);

      // Mixing legacy and not legacy path
      String p6 = ".key3";
      String p7 = ".key4";
      String p8 = ".key5";
      JsonPath jp6 = new JsonPath(p6);
      JsonPath jp7 = new JsonPath(p7);
      JsonPath jp8 = new JsonPath(p8);

      result = new DefaultJsonParser().createJsonValue(command.jsonGet(k(), p1, p2, p6));
      assertThat(compareJSONGet(result, jv, jp1, jp2, jp6)).isEqualTo(true);

      result = new DefaultJsonParser().createJsonValue(command.jsonGet(k(), p1, p2, p7));
      assertThat(compareJSONGet(result, jv, jp1, jp2, jp7)).isEqualTo(true);

      assertThatThrownBy(() -> { command.jsonGet(k(), p1, p2, p8);
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageStartingWith("ERR ");

      // Multiple path multiple results, legacy and not
      result = new DefaultJsonParser().createJsonValue(command.jsonGet(k(), p4, p6));
      assertThat(compareJSONGet(result, jv, jp4, jp6)).isEqualTo(true);
   }

   @Test
   public void testJSONGETLegacyError() {
      CustomStringCommands command = CustomStringCommands.instance(redisConnection);
      assertThatThrownBy(() -> {
         command.jsonSet(k(), "..", "{ \"k1\": \"v1\"}");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageStartingWith("ERR ");
   }

   @Test
   public void testJSONGETPrettyPrinter() {
      JsonPath jp = new JsonPath("$");
      JsonValue jv = new DefaultJsonParser().createJsonValue("""
               { "key1":"value1",
                 "key2":"value2"
               }
            """);
      String key = k();
      assertThat(redis.jsonSet(key, jp, jv)).isEqualTo("OK");
      JsonPath jp1 = new JsonPath("$");
      JsonGetArgs args = new JsonGetArgs().indent("1").newline("2").space("3");
      var result = redis.jsonGet(key, args, jp1);
      assertThat(result).hasSize(1);
      String strResult = result.get(0).toString();
      String expected = """
            [21{211"key1":3"value1",211"key2":3"value2"21}2]""";
      assertThat(strResult).isEqualTo(expected);
   }

   @Test
   public void testJSONSETWhiteSpaces() {
      String value = """
            { \t"k1"\u000d:\u000a"v1"}
             """;
      JsonValue jv = new DefaultJsonParser().createJsonValue(value);
      JsonPath jp = new JsonPath("$");
      assertThat(redis.jsonSet(k(), jp, jv)).isEqualTo("OK");
      List<JsonValue> result = redis.jsonGet(k(), new JsonPath("$"));
      assertThat(result).hasSize(1);
      assertThat(compareJSON(result.get(0), jv)).isTrue();
   }

   private boolean compareJSONGet(JsonValue result, JsonValue expected, JsonPath... paths) {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode rootObjectNode, resultNode;
      if (paths == null) {
         paths = new JsonPath[] { new JsonPath("$") };
      }
      try {
         rootObjectNode = (JsonNode) mapper.readTree(expected.toString());
         resultNode = (JsonNode) mapper.readTree(result.toString());
         var jpCtx = com.jayway.jsonpath.JsonPath.using(JSONUtil.configForGet).parse(rootObjectNode);
         boolean isLegacy = true;
         // If all paths are legacy, return results in legacy mode. i.e. no array
         for (JsonPath path : paths) {
            isLegacy &= !JSONUtil.isJsonPath(path.toString());
         }
         if (paths.length == 1) {
            // jpctx.read doesn't like legacy ".", change it to "$". everything else seems
            // to work
            String pathStr = ".".equals(paths[0].toString()) ? "$" : paths[0].toString();
            JsonNode node = isLegacy ? ((ArrayNode) jpCtx.read(pathStr)).get(0)
                  : jpCtx.read(pathStr);
            return resultNode.equals(node);
         }
         ObjectNode root = mapper.createObjectNode();
         for (JsonPath path : paths) {
            String pathStr = path.toString();
            JsonNode node = isLegacy ? ((ArrayNode) jpCtx.read(pathStr)).get(0)
                  : jpCtx.read(pathStr);
            root.set(pathStr, node);
         }
         return resultNode.equals(root);
      } catch (Exception ex) {
         throw new RuntimeException(ex);
      }
   }

   private boolean compareJSON(JsonValue result, JsonValue expected) {
      return compareJSONGet(result, expected, (JsonPath[]) null);
   }

   private boolean compareJSONSet(JsonValue newDoc, JsonValue oldDoc, String path, JsonValue node) {
      ObjectMapper mapper = new ObjectMapper();
      try {
         var newRootNode = mapper.readTree(unwrapIfArray(newDoc).toString());
         var oldRootNode = mapper.readTree(unwrapIfArray(oldDoc).toString());
         // Unwrap objects if in an array
         var jpCtx = com.jayway.jsonpath.JsonPath.using(JSONUtil.configForSet).parse(oldRootNode);
         var pathStr = new String(path);
         var newNode = mapper.readTree(node.toString());
         jpCtx.set(pathStr, newNode);
         // Check the whole doc is correct
         if (!oldRootNode.equals(newRootNode)) {
            return false;
         }
         // Check the node is set correctly
         var newJpCtx = com.jayway.jsonpath.JsonPath.using(JSONUtil.configForSet).parse(newRootNode);
         var newNodeFromNewDoc = newJpCtx.read(pathStr);
         var expectedNode = jpCtx.read(pathStr);
         return newNodeFromNewDoc.equals(expectedNode);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private JsonValue wrapInArray(JsonValue v) {
      JsonArray arr = new DefaultJsonParser().createJsonArray();
      return arr.add(v);
   }

   private JsonValue unwrapIfArray(JsonValue v) {
      if (v.isJsonArray()) {
         var arr = v.asJsonArray();
         if (arr.size() == 0)
            return null;
         if (arr.size() == 1)
            return arr.get(0);
         throw new RuntimeException("Argument has size > 1");
      }
      return v;
   }
}
