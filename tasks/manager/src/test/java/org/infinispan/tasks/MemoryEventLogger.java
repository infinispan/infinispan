package org.infinispan.tasks;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;
import org.infinispan.util.logging.events.EventLogger;

/**
 * @author Tristan Tarrant
 * @since 8.2
 */
public class MemoryEventLogger implements EventLogger {
    EventLogCategory category;
    String context;
    String detail;
    EventLogLevel level;
    String message;
    String scope;
    String who;

    public MemoryEventLogger(EmbeddedCacheManager cacheManager, TimeService timeService) {
        reset();
    }

    @Override
    public void log(EventLogLevel level, EventLogCategory category, String message) {
        this.level = level;
        this.category = category;
        this.message = message;
    }

    @Override
    public List<EventLog> getEvents(Instant start, int count, Optional<EventLogCategory> category, Optional<EventLogLevel> level) {
        return Collections.emptyList();
    }

    @Override
    public EventLogger scope(String scope) {
        this.scope = scope;
        return this;
    }

    @Override
    public EventLogger context(String context) {
        this.context = context;
        return this;
    }

    @Override
    public EventLogger detail(String detail) {
        this.detail = detail;
        return this;
    }

    @Override
    public EventLogger who(String who) {
        this.who = who;
        return this;
    }

    void reset() {
        category = null;
        context = null;
        detail = null;
        level = null;
        message = null;
        scope = null;
        who = null;
    }

    public EventLogCategory getCategory() {
        return category;
    }

    public String getContext() {
        return context;
    }

    public String getDetail() {
        return detail;
    }

    public EventLogLevel getLevel() {
        return level;
    }

    public String getMessage() {
        return message;
    }

    public String getScope() {
        return scope;
    }

    public String getWho() {
        return who;
    }
}
