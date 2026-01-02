package org.infinispan.server.resp.commands.sortedset;

import static org.assertj.core.api.Assertions.assertThat;

import org.testng.annotations.Test;

public class ZSetCommonUtilsTest {

    @Test
    public void testWithScoresArg() {
        assertThat(ZSetCommonUtils.isWithScoresArg("withscores".getBytes())).isTrue();
        assertThat(ZSetCommonUtils.isWithScoresArg("WITHSCORES".getBytes())).isTrue();
        assertThat(ZSetCommonUtils.isWithScoresArg("WIthScoreS".getBytes())).isTrue();
        assertThat(ZSetCommonUtils.isWithScoresArg("withscore".getBytes())).isFalse();
        assertThat(ZSetCommonUtils.isWithScoresArg("WITHSCORE".getBytes())).isFalse();
    }

}
