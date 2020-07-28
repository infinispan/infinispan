public interface XSiteEntryMergePolicy<K, V> {
   CompletionStage<SiteEntry<V>> mrege(K key, SiteEntry<V> localEntry, SiteEntry<V> remoteEntry);
}
