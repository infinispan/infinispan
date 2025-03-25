package org.infinispan.rest;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "rest.InvocationHelperTest")
public class InvocationHelperTest {

    @Test
    public void testBaseUrlFormat() {
        assertEquals("", InvocationHelper.createURLForCSPHeader(null));
        assertEquals("https://example.com", InvocationHelper.createURLForCSPHeader("https://example.com"));
        assertEquals("https://localhost:11222", InvocationHelper.createURLForCSPHeader("https://localhost:11222"));
        assertEquals("", InvocationHelper.createURLForCSPHeader("xxxx"));
    }
}
