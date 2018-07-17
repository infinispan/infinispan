package org.infinispan.marshall.core;

import static java.util.Objects.deepEquals;
import static org.infinispan.marshall.core.Primitives.ID_BOOLEAN_ARRAY;
import static org.infinispan.marshall.core.Primitives.ID_BOOLEAN_OBJ;
import static org.infinispan.marshall.core.Primitives.ID_BYTE_ARRAY;
import static org.infinispan.marshall.core.Primitives.ID_BYTE_OBJ;
import static org.infinispan.marshall.core.Primitives.ID_CHAR_ARRAY;
import static org.infinispan.marshall.core.Primitives.ID_CHAR_OBJ;
import static org.infinispan.marshall.core.Primitives.ID_DOUBLE_ARRAY;
import static org.infinispan.marshall.core.Primitives.ID_DOUBLE_OBJ;
import static org.infinispan.marshall.core.Primitives.ID_FLOAT_ARRAY;
import static org.infinispan.marshall.core.Primitives.ID_FLOAT_OBJ;
import static org.infinispan.marshall.core.Primitives.ID_INT_ARRAY;
import static org.infinispan.marshall.core.Primitives.ID_INT_OBJ;
import static org.infinispan.marshall.core.Primitives.ID_LONG_ARRAY;
import static org.infinispan.marshall.core.Primitives.ID_LONG_OBJ;
import static org.infinispan.marshall.core.Primitives.ID_SHORT_ARRAY;
import static org.infinispan.marshall.core.Primitives.ID_SHORT_OBJ;
import static org.infinispan.marshall.core.Primitives.ID_STRING;
import static org.infinispan.marshall.core.Primitives.readPrimitive;
import static org.infinispan.marshall.core.Primitives.writePrimitive;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.Exceptions;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Created by karesti on 22/03/17.
 */
@Test(groups = "functional", testName = "marshall.PrimitivesTest")
public class PrimitivesTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cm;

   private  GlobalMarshaller globalMarshaller;

   @BeforeClass
   public void setUp() {
      cm = TestCacheManagerFactory.createCacheManager();
      globalMarshaller = TestingUtil.extractGlobalMarshaller(cm);
   }

   @AfterClass
   public void tearDown() {
      if (cm != null) cm.stop();
   }

   public void testReadAndWrite() throws Exception {
      assertReadAndWrite(new byte[]{0, 1}, ID_BYTE_ARRAY);
      assertReadAndWrite("kaixo", ID_STRING);
      assertReadAndWrite(true, ID_BOOLEAN_OBJ);
      assertReadAndWrite((byte) 0, ID_BYTE_OBJ);
      assertReadAndWrite('P', ID_CHAR_OBJ);
      assertReadAndWrite(123d, ID_DOUBLE_OBJ);
      assertReadAndWrite(123f, ID_FLOAT_OBJ);
      assertReadAndWrite(123, ID_INT_OBJ);
      assertReadAndWrite(123L, ID_LONG_OBJ);
      assertReadAndWrite((short) 123, ID_SHORT_OBJ);
      assertReadAndWrite(new boolean[]{true, false}, ID_BOOLEAN_ARRAY);
      assertReadAndWrite(new char[]{'k', 'a', 'i', 'x', 'o'}, ID_CHAR_ARRAY);
      assertReadAndWrite(new double[]{123d, 456d}, ID_DOUBLE_ARRAY);
      assertReadAndWrite(new float[]{123f, 456f}, ID_FLOAT_ARRAY);
      assertReadAndWrite(new int[]{123, 456}, ID_INT_ARRAY);
      assertReadAndWrite(new long[]{123L, 456L}, ID_LONG_ARRAY);
      assertReadAndWrite(new short[]{123, 456}, ID_SHORT_ARRAY);

      Exceptions.expectException(IOException.class, "Unknown primitive type: diable",
            () -> writePrimitive("diable", new BytesObjectOutput(10240, globalMarshaller), 666));
   }

   private void assertReadAndWrite(Object write, int id) throws IOException, ClassNotFoundException {
      BytesObjectOutput out = new BytesObjectOutput(10240, globalMarshaller);
      writePrimitive(write, out, id);
      Object read = readPrimitive(BytesObjectInput.from(out.bytes, 0, globalMarshaller, null));
      assertTrue(deepEquals(write, read));
   }
}
