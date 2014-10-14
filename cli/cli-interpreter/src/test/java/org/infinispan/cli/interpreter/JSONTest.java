package org.infinispan.cli.interpreter;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectMapper.DefaultTyping;
import org.testng.annotations.Test;

@Test(groups = "functional", testName="cli-server.JSONTest")
public class JSONTest {

   public void testJSONMapping() throws JsonGenerationException, JsonMappingException, IOException {
      MyClass x = new MyClass();
      x.i = 5;
      x.s = "abc";
      x.b = true;
      x.x = new MyClass();

      ObjectMapper objMapper = new ObjectMapper().enableDefaultTyping(DefaultTyping.NON_FINAL, JsonTypeInfo.As.WRAPPER_OBJECT);
      String s = objMapper.writeValueAsString(x);
      System.out.println(s);
      Object readValue = objMapper.readValue(s, Object.class);
      System.out.println(readValue.getClass().getName());
   }
}
