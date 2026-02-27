package org.infinispan.cli.completers;

import java.util.List;
import java.util.function.Supplier;

import org.aesh.readline.SuggestionProvider;
import org.aesh.readline.history.History;
import org.aesh.terminal.utils.Parser;

/**
 * Provides inline ghost text suggestions based on command history.
 * Searches history from most recent to oldest, returning the suffix
 * of the first matching entry.
 */
public class HistorySuggestionProvider implements SuggestionProvider {

    private final Supplier<History> historySupplier;

    public HistorySuggestionProvider(Supplier<History> historySupplier) {
        this.historySupplier = historySupplier;
    }

    @Override
    public String suggest(String buffer) {
        if (buffer == null || buffer.isEmpty()) {
            return null;
        }
        History history = historySupplier.get();
        if (history == null) {
            return null;
        }
        List<int[]> entries = history.getAll();
        // Search from most recent entry backwards
        for (int i = entries.size() - 1; i >= 0; i--) {
            String entry = Parser.fromCodePoints(entries.get(i));
            if (entry.startsWith(buffer) && entry.length() > buffer.length()) {
                return entry.substring(buffer.length());
            }
        }
        return null;
    }
}
