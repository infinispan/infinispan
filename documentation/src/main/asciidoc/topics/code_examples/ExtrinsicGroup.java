public interface Grouper<T> {
    String computeGroup(T key, String group);

    Class<T> getKeyType();
}
