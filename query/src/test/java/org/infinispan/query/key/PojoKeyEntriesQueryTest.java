package org.infinispan.query.key;

import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.model.Game;
import org.infinispan.query.model.GameKey;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

@Test(groups = "functional", testName = "query.key.PojoKeyEntriesQueryTest")
public class PojoKeyEntriesQueryTest extends SingleCacheManagerTest {

    @Override
    protected EmbeddedCacheManager createCacheManager() {
        ConfigurationBuilder indexed = new ConfigurationBuilder();
        indexed.indexing().enable()
                .storage(LOCAL_HEAP)
                .addIndexedEntity(Game.class);

        return TestCacheManagerFactory.createCacheManager(indexed);
    }

    @BeforeMethod
    public void setUp() {
        if (!cache.isEmpty()) {
            return;
        }

        Game game1 = new Game("game-1", "description of the game one");
        Game game2 = new Game("game-2", "description of the game two");
        Game game3 = new Game("game-3", "description of the game three");
        Game game4 = new Game("game-4", "description of the game four");
        Game game5 = new Game("game-5", "description of the game five");

        cache.put(new GameKey(2020, game1.getName()), game1);
        cache.put(new GameKey(2020, game2.getName()), game2);
        cache.put(new GameKey(2020, game3.getName()), game3);
        cache.put(new GameKey(2020, game4.getName()), game4);
        cache.put(new GameKey(2020, game5.getName()), game5);
    }

    @Test
    public void test() {
        Query<Game> query = cache.query("from org.infinispan.query.model.Game");
        List<Game> list = query.list();
        assertThat(list).hasSize(5);
    }
}
