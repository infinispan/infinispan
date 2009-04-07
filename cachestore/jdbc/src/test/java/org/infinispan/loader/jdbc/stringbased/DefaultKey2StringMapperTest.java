package org.infinispan.loader.jdbc.stringbased;

import org.testng.annotations.Test;

/**
 * Tester for {@link org.infinispan.loader.jdbc.stringbased.Key2StringMapper}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "unit", testName = "loader.jdbc.stringbased.DefaultKey2StringMapperTest")
public class DefaultKey2StringMapperTest {

   DefaultKey2StringMapper mapper = new DefaultKey2StringMapper();

   public void testPrimitivesAreSupported() {
      assert mapper.isSupportedType(Integer.class);
      assert mapper.isSupportedType(Byte.class);
      assert mapper.isSupportedType(Short.class);
      assert mapper.isSupportedType(Long.class);
      assert mapper.isSupportedType(Double.class);
      assert mapper.isSupportedType(Float.class);
      assert mapper.isSupportedType(Boolean.class);
      assert mapper.isSupportedType(String.class);
   }

   @SuppressWarnings(value = "all")
   public void testGetStingMapping() {
      Object[] toTest = {new Integer(0), new Byte("1"), new Short("2"), new Long(3), new Double("3.4"), new Float("3.5"), Boolean.FALSE, "some string"};
      for (Object o : toTest) {
         assert mapper.getStringMapping(o).equals(o.toString());
      }
   }
}
