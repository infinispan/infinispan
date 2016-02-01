package org.infinispan.scripting;

import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Arrays;

import org.infinispan.scripting.impl.ScriptMetadata;
import org.infinispan.scripting.impl.ScriptMetadataParser;
import org.testng.annotations.Test;

@Test(groups="functional", testName="scripting.ScriptMetadataTest")
public class ScriptMetadataTest {

   public void testDoubleSlashComment() throws Exception {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.js", "// name=test");
      assertEquals("test", metadata.name());
   }

   public void testHashComment() throws Exception {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.js", "# name=test");
      assertEquals("test", metadata.name());
   }

   public void testDoublSemicolonComment() throws Exception {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.js", ";; name=test");
      assertEquals("test", metadata.name());
   }

   public void testMultiplePairs() throws Exception {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.scala", "// name=test,language=scala");
      assertEquals("test", metadata.name());
      assertEquals("scala", metadata.language().get());
   }

   public void testDoubleQuotedValues() throws Exception {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.scala", "// name=\"te,st\",language=scala");
      assertEquals("te,st", metadata.name());
      assertEquals("scala", metadata.language().get());
   }

   public void testSingleQuotedValues() throws Exception {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.scala", "// name='te,st',language=scala");
      assertEquals("te,st", metadata.name());
      assertEquals("scala", metadata.language().get());
   }

   public void testArrayValues() throws Exception {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.scala", "// name=test,language=javascript,parameters=[a,b,c]");
      assertEquals("test", metadata.name());
      assertEquals("javascript", metadata.language().get());
      assertTrue(metadata.parameters().containsAll(Arrays.asList("a", "b", "c")));
   }

   public void testMultiLine() throws Exception {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.scala", "// name=test\n// language=scala");
      assertEquals("test", metadata.name());
      assertEquals("scala", metadata.language().get());
   }

   @Test(expectedExceptions=IllegalArgumentException.class, expectedExceptionsMessageRegExp="^ISPN026011.*")
   public void testBrokenParameters() throws Exception {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.scala", "// name=test,language=javascript,parameters=\"a,b,c\"");
      assertEquals("test", metadata.name());
      assertEquals("javascript", metadata.language());
      assertTrue(metadata.parameters().containsAll(Arrays.asList("a", "b", "c")));
   }
}
