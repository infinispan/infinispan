package org.infinispan.scripting;

import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.infinispan.commons.CacheException;
import org.infinispan.scripting.impl.ScriptMetadata;
import org.infinispan.scripting.impl.ScriptMetadataParser;
import org.testng.annotations.Test;

@Test(groups="functional", testName="scripting.ScriptMetadataTest")
public class ScriptMetadataTest {

   public void testDoubleSlashComment() throws Exception {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.js", "// name=test");
      assertEquals("test", metadata.name());
   }

   public void testDefaultScriptExtension() throws Exception {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test", "// name=test");
      assertEquals("test", metadata.name());
   }

   public void testDefaultScriptExtension1() {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.", "/* name=exampleName */");
      assertEquals("test.", metadata.name());
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

   public void testSingleQuatedValuesWithProvidedExtension() throws Exception {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test", "// name='te,st',language=scala,extension=scala");
      assertEquals("te,st", metadata.name());
      assertEquals("scala", metadata.language().get());
      assertEquals("scala", metadata.extension());
   }

   public void testDataTypeUtf8() throws Exception {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test", "// name='test',language=javascript,datatype='text/plain; charset=utf-8'");
      assertEquals("test", metadata.name());
      assertEquals("javascript", metadata.language().get());
      assertEquals("js", metadata.extension());
      assertTrue(metadata.dataType().match(TEXT_PLAIN));
      assertEquals(StandardCharsets.UTF_8, metadata.dataType().getCharset());
   }

   public void testDataTypeOther() throws Exception {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test", "// name='test',language=javascript,datatype='text/plain; charset=us-ascii'");
      assertEquals("test", metadata.name());
      assertEquals("javascript", metadata.language().get());
      assertEquals("js", metadata.extension());
      assertTrue(metadata.dataType().match(TEXT_PLAIN));
      assertEquals(StandardCharsets.US_ASCII, metadata.dataType().getCharset());
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

   @Test(expectedExceptions=IllegalArgumentException.class, expectedExceptionsMessageRegExp=".*Script parameters must be declared using.*")
   public void testBrokenParameters() throws Exception {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.scala", "// name=test,language=javascript,parameters=\"a,b,c\"");
      assertEquals("test", metadata.name());
      assertEquals("javascript", metadata.language());
      assertTrue(metadata.parameters().containsAll(Arrays.asList("a", "b", "c")));
   }

   @Test(expectedExceptions=CacheException.class, expectedExceptionsMessageRegExp=".*Unknown script mode:.*")
   public void testUnknownScriptProperty() throws Exception {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.scala", "// name=test,language=javascript,parameters=[a,b,c],unknown=example");
      assertEquals("test", metadata.name());
      assertEquals("javascript", metadata.language());
      assertTrue(metadata.parameters().containsAll(Arrays.asList("a", "b", "c")));
   }
}
