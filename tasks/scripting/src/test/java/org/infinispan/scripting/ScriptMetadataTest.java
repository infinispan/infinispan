package org.infinispan.scripting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import org.infinispan.commons.CacheException;
import org.infinispan.scripting.impl.ScriptMetadata;
import org.infinispan.scripting.impl.ScriptMetadataParser;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups="functional", testName="scripting.ScriptMetadataTest")
public class ScriptMetadataTest extends AbstractInfinispanTest {

   public void testDoubleSlashComment() {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.js", "// name=test").build();
      assertEquals("test", metadata.name());
   }

   public void testDefaultScriptExtension() {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test", "// name=test").build();
      assertEquals("test", metadata.name());
   }

   public void testDefaultScriptExtension1() {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.", "/* name=exampleName */").build();
      assertEquals("test.", metadata.name());
   }

   public void testHashComment() {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.js", "# name=test").build();
      assertEquals("test", metadata.name());
   }

   public void testDoublSemicolonComment() {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.js", ";; name=test").build();
      assertEquals("test", metadata.name());
   }

   public void testMultiplePairs() {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.scala", "// name=test,language=scala").build();
      assertEquals("test", metadata.name());
      assertEquals("scala", metadata.language().get());
   }

   public void testDoubleQuotedValues() {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.scala", "// name=\"te,st\",language=scala").build();
      assertEquals("te,st", metadata.name());
      assertEquals("scala", metadata.language().get());
   }

   public void testSingleQuotedValues() {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.scala", "// name='te,st',language=scala").build();
      assertEquals("te,st", metadata.name());
      assertEquals("scala", metadata.language().get());
   }

   public void testSingleQuatedValuesWithProvidedExtension() throws Exception {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test", "// name='te,st',language=scala,extension=scala").build();
      assertEquals("te,st", metadata.name());
      assertEquals("scala", metadata.language().get());
      assertEquals("scala", metadata.extension());
   }

   public void testDataTypeUtf8() {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test", "// name='test',language=javascript,datatype='text/plain; charset=utf-8'").build();
      assertEquals("test", metadata.name());
      assertEquals("javascript", metadata.language().get());
      assertEquals("js", metadata.extension());
      assertTrue(metadata.dataType().match(TEXT_PLAIN));
      assertEquals(StandardCharsets.UTF_8, metadata.dataType().getCharset());
   }

   public void testDataTypeOther() {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test", "// name='test',language=javascript,datatype='text/plain; charset=us-ascii'").build();
      assertEquals("test", metadata.name());
      assertEquals("javascript", metadata.language().get());
      assertEquals("js", metadata.extension());
      assertTrue(metadata.dataType().match(TEXT_PLAIN));
      assertEquals(StandardCharsets.US_ASCII, metadata.dataType().getCharset());
   }

   public void testArrayValues() {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.scala", "// name=test,language=javascript,parameters=[a,b,c]").build();
      assertEquals("test", metadata.name());
      assertEquals("javascript", metadata.language().get());
      assertThat(metadata.parameters()).containsExactly("a", "b", "c");
   }

   public void testProperties() {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.scala", "// name=test,language=javascript,properties={k1:v1,k2:v2}").build();
      assertEquals("test", metadata.name());
      assertEquals("javascript", metadata.language().get());
      assertThat(metadata.properties()).containsExactlyEntriesOf(Map.of("k1", "v1", "k2", "v2"));
   }

   public void testMultiLine() {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.scala", "// name=test\n// language=scala").build();
      assertEquals("test", metadata.name());
      assertEquals("scala", metadata.language().get());
   }

   @Test(expectedExceptions=IllegalArgumentException.class, expectedExceptionsMessageRegExp=".*Script parameters must be declared using.*")
   public void testBrokenParameters() {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.scala", "// name=test,language=javascript,parameters=\"a,b,c\"").build();
      assertEquals("test", metadata.name());
      assertEquals("javascript", metadata.language().get());
      assertTrue(metadata.parameters().containsAll(Arrays.asList("a", "b", "c")));
   }

   @Test(expectedExceptions=CacheException.class, expectedExceptionsMessageRegExp=".*Unknown script mode:.*")
   public void testUnknownScriptProperty() {
      ScriptMetadata metadata = ScriptMetadataParser.parse("test.scala", "// name=test,language=javascript,parameters=[a,b,c],unknown=example").build();
      assertEquals("test", metadata.name());
      assertEquals("javascript", metadata.language().get());
      assertTrue(metadata.parameters().containsAll(Arrays.asList("a", "b", "c")));
   }
}
