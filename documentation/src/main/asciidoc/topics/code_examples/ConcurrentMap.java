public interface Cache<K, V> extends BasicCache<K, V> {
 ...
}

public interface BasicCache<K, V> extends ConcurrentMap<K, V> {
 ...
}
