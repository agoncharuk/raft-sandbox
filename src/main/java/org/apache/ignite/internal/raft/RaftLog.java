/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.raft;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.raft.message.LogEntry;
import org.apache.ignite.internal.raft.message.MessageFactory;
import org.apache.ignite.internal.raft.service.TermRange;
import org.apache.ignite.internal.raft.service.PersistenceService;

/**
 * Wrapper over persistence service to hold volatile state for the replicator during protocol execution.
 */
class RaftLog {
    /** */
    private long offs;

    /** */
    private List<LogEntry> unstable = new ArrayList<>();

    /** */
    private final PersistenceService persistenceSvc;

    /** */
    private final MessageFactory msgFactory;

    /**
     */
    RaftLog(PersistenceService persistenceSvc, MessageFactory msgFactory) {
        this.persistenceSvc = persistenceSvc;
        this.msgFactory = msgFactory;

        offs = persistenceSvc.lastLogIndex();
    }

    /**
     * @return The index of the last logged entry.
     */
    long lastLogIndex() {
        return offs + unstable.size();
    }

    /**
     * Creates log entries from the passed user objects and appends them to the unstable list.
     * The log may not accept all the entries if the number of pending entries exceeds the quota.
     *
     * @param entries Entries to append to the log.
     * @return Array of indexes for accepted entries, starting from the first one.
     */
    <T> long[] addUnstable(long term, T[] entries) {
        // TODO add max unpersisted entries tracking.
        long[] res = new long[entries.length];

        for (int i = 0; i < entries.length; i++) {
            T entry = entries[i];

            long idx = offs + unstable.size() + i + 1;

            unstable.add(msgFactory.createLogEntry(entry, term, idx));

            res[i] = idx;
        }

        return res;
    }

    /**
     * Appends entries to the unstable list. The unstable entries will be captured by the progress.
     * The log may not accept all the entries if the number of pending entries exceeds the quota.
     *
     * @param entries Entries sent by the remote leader.
     * @return The index of the last appended entry.
     */
    long addUnstable(LogEntry[] entries) {
        // TODO add max unpersisted entries tracking.
        for (LogEntry entry : entries) {
            if (entry.index() > offs + unstable.size() + 1)
                // TODO this may be a recoverable exception.
                throw new UnrecoverableException("Adding out-of-order entry [index=" + entry.index() +
                    ", expected=" + (offs + unstable.size() + 1) + ']');
            // A duplicate, ignore this entry.
            else if (entry.index() < offs + unstable.size() + 1)
                continue;

            unstable.add(entry);
        }

        return lastLogIndex();
    }

    /**
     * @param logIdx Log index to query.
     * @return Entry term for the given log index.
     */
    long entryTerm(long logIdx) {
        if (logIdx == 0)
            return 0;

        if (logIdx <= offs)
            return persistenceSvc.entryTerm(logIdx);

        return unstable.get((int)(logIdx - offs - 1)).term();
    }

    /**
     */
    LogEntry[] fetchEntries(long idx, long batchSize) {
        if (offs > persistenceSvc.lastLogIndex())
            throw new UnrecoverableException("Log is desynced with persistence [offset=" + offs +
                ", lastLogIndex=" + persistenceSvc.lastLogIndex() + ']');

        if (idx > lastLogIndex())
            throw new IllegalArgumentException();

        long available = lastLogIndex() - idx + 1;

        if (available > Integer.MAX_VALUE - 8)
            throw new IllegalArgumentException();

        if (batchSize == 0)
            batchSize = available;

        int entries = (int)Math.min(available, batchSize);

        LogEntry[] res = new LogEntry[entries];

        int start = 0;

        if (idx <= persistenceSvc.lastLogIndex()) {
            // This will fetch persistenceSvc.lastLogIndex() - idx entries.
            persistenceSvc.fetchEntries(idx, res);

            start = (int)(persistenceSvc.lastLogIndex() - idx + 1);

            idx += start;
        }

        for (int i = 0; i < entries - start; i++)
            res[start + i] = unstable.get((int)(idx - offs + i - 1));

        return res;
    }

    /**
     */
    TermRange fetchTermStart(long idx) {
        if (offs > persistenceSvc.lastLogIndex())
            throw new UnrecoverableException("Log is desynced with persistence [offset=" + offs +
                ", lastLogIndex=" + persistenceSvc.lastLogIndex() + ']');

        if (lastLogIndex() == 0)
            return new TermRange(0, 0, 0);

        if (idx > lastLogIndex())
            idx = lastLogIndex();

        if (idx <= offs)
            return persistenceSvc.fetchTermRange(idx);

        int i = (int)(idx - offs - 1);
        long term = unstable.get(i).term();

        while (i >= 0 && unstable.get(i).term() == term)
            i--;

        if (i < 0 && offs > 0 && persistenceSvc.entryTerm(offs) == term) {
            TermRange range = persistenceSvc.fetchTermRange(offs);

            return new TermRange(range.startIndex(), idx, term);
        }

        return new TermRange(offs + i + 2, idx, term);
    }

    /**
     * @return {@code true} if there are unstable entries needed to be persisted to disk.
     */
    boolean hasProgress() {
        return !unstable.isEmpty();
    }

    /**
     * Captures the unstable entries to be passed to the progress object. Will advance the internal offset index
     * and clear the pending list.
     *
     * @param progress Progress object to fill with pending log entries.
     */
    void fillProgress(Progress progress) {
        if (!unstable.isEmpty()) {
            progress.setLogEntries(Collections.unmodifiableList(unstable));

            offs += unstable.size();

            unstable = new ArrayList<>();
        }
    }

    /**
     * Virtually truncates the log by clearing the volatile entries and shifting offset if necessary. We rely
     * on the replicator user to truncate log accordingly when a first entry to be persisted after offset
     * is passed in progress object.
     *
     * @param idx Start index (inclusive) to truncate. After this call last existing entry will have
     *      the index {@code idx - 1}.
     */
    void truncate(long idx) {
        if (idx <= offs) {
            offs = idx - 1;

            unstable.clear();
        }
        else {
            int endIdx = (int)(idx - offs - 1);

            if (endIdx < unstable.size())
                unstable = unstable.subList(0, endIdx);
        }
    }
}
