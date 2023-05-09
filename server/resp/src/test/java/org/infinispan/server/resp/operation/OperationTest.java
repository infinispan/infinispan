package org.infinispan.server.resp.operation;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;

import org.testng.annotations.Test;


@Test(groups = "unit", testName = "server.resp.operation.OperationTest")
public class OperationTest {
@Test
public void testLcs() {
    byte[] val1 =    "ohmytext".getBytes(StandardCharsets.US_ASCII);
    byte[] val2 =    "mynewtext".getBytes(StandardCharsets.US_ASCII);

    var lcsOpts = new LCSOperation.LCSOperationContext(val1, val2,false, false, false, 0);
    lcsOpts.lcsLength(val1, val2);
    lcsOpts.backtrack(val1, val2);
    assertThat(lcsOpts.getResult().lcs).isEqualTo(new byte[] {'m','y','t','e','x','t'});
}
}
