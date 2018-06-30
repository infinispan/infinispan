package org.infinispan.cli.interpreter;

import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;

import org.testng.annotations.Test;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Test(groups = "functional", testName="cli.interpreter.JSONTest")
public class JSONTest {

   public void testJSONMapping() throws JsonGenerationException, JsonMappingException, IOException {
      MyClass x = new MyClass();
      x.i = 5;
      x.s = "abc";
      x.b = true;
      x.x = new MyClass();

      ObjectMapper objMapper = new ObjectMapper().enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.WRAPPER_OBJECT);
      String s = objMapper.writeValueAsString(x);
      Object readValue = objMapper.readValue(s, Object.class);
      assertEquals(x.toString(), readValue.toString());
      assertEquals(s, objMapper.writeValueAsString(readValue));
   }
}
