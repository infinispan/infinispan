package org.infinispan.rest;

import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.Test;

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
