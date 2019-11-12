package org.infinispan.commons.dataconversion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * @since 10.1
 */
public class MediaTypeResolverTest {

   @Test
   public void testResolver() {
      assertNull(MediaTypeResolver.getMediaType(null));
      assertNull(MediaTypeResolver.getMediaType("noextension"));
      assertNull(MediaTypeResolver.getMediaType("124."));
      assertEquals("application/javascript", MediaTypeResolver.getMediaType("file.js"));
      assertEquals("image/jpeg", MediaTypeResolver.getMediaType("file.jpg"));
      assertEquals("image/jpeg", MediaTypeResolver.getMediaType("file.jpeg"));
      assertEquals("image/jpeg", MediaTypeResolver.getMediaType("file.jpe"));
      assertEquals("text/css", MediaTypeResolver.getMediaType("file.css"));
      assertEquals("text/html", MediaTypeResolver.getMediaType("file.htm"));
      assertEquals("text/html", MediaTypeResolver.getMediaType("file.html"));
      assertEquals("application/java-archive", MediaTypeResolver.getMediaType("file.JAR"));
   }
}
