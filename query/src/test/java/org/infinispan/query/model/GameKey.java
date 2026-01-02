package org.infinispan.query.model;

import java.util.Objects;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.query.Transformable;
import org.infinispan.query.Transformer;

@Indexed
@Transformable(transformer = GameKey.GameKeyTransformer.class)
public class GameKey {

    private final Integer year;
    private final String name;

    @ProtoFactory
    public GameKey(Integer year, String name) {
        this.year = year;
        this.name = name;
    }

    @Basic(projectable = true, sortable = true)
    @ProtoField(1)
    public Integer getYear() {
        return year;
    }

    @Keyword(projectable = true, sortable = true)
    @ProtoField(2)
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "GameKey{" +
                "year=" + year +
                ", name='" + name + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GameKey gameKey = (GameKey) o;
        return Objects.equals(year, gameKey.year) && Objects.equals(name, gameKey.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, name);
    }

    public static class GameKeyTransformer implements Transformer {
        private static final String SEPARATOR = "#";

        @Override
        public Object fromString(String string) {
            int separation = string.indexOf(SEPARATOR);
            if (separation == -1) {
                return new GameKey(0, "");
            }

            Integer year = Integer.parseInt(string.substring(0, separation));
            String name = string.substring(separation + 1);
            return new GameKey(year, name);
        }

        @Override
        public String toString(Object obj) {
            GameKey casted = (GameKey) obj;
            return casted.getYear() + SEPARATOR + casted.getName();
        }
    }
}
