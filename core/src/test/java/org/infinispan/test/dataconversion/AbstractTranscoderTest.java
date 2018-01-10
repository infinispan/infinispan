package org.infinispan.test.dataconversion;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(groups = "functional", testName = "rest.TranscoderTest")
public abstract class AbstractTranscoderTest {
   protected Transcoder transcoder;
   protected Set<MediaType> supportedMediaTypes;

   @Test
   public void testTranscoderSupportedMediaTypes() {
      List<MediaType> supportedMediaTypesList = new ArrayList<>(supportedMediaTypes);
      assertTrue(supportedMediaTypesList.size() >= 2, "Must be at least 2 supported MediaTypes");
      assertFalse(supportedMediaTypesList.get(0).match(supportedMediaTypesList.get(1)), "Supported MediaTypes Must be different");
   }

   @Test
   public abstract void testTranscoderTranscode() throws Exception;
}
