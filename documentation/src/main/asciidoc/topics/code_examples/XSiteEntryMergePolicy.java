public interface XSiteEntryMergePolicy<K, V> {
   CompletionStage<SiteEntry<V>> merge(K key, SiteEntry<V> localEntry, SiteEntry<V> remoteEntry);
}
