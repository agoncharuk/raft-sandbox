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

package org.apache.ignite.internal.raft.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.raft.UnrecoverableException;
import org.apache.ignite.internal.raft.message.LogEntry;

/**
 *
 */
public class SimulatedPersistenceService implements PersistenceService {
    /** */
    private ArrayList<LogEntry> log = new ArrayList<>();

    /** */
    private HardState hardState;

    /** */
    private ConfigurationState confState;

    /**
     */
    public SimulatedPersistenceService() {
    }

    /**
     */
    public SimulatedPersistenceService(
        HardState hardState,
        ConfigurationState confState,
        LogEntry... entries
    ) {
        this.hardState = hardState;
        this.confState = confState;

        log.addAll(Arrays.asList(entries));
    }

    /** {@inheritDoc} */
    @Override public HardState hardState() {
        return hardState;
    }

    /**
     * @param hardState Hard state to save.
     */
    public void hardState(HardState hardState) {
        this.hardState = hardState;
    }

    /** {@inheritDoc} */
    @Override public ConfigurationState confState() {
        return confState;
    }

    /** {@inheritDoc} */
    @Override public long lastLogIndex() {
        return log.size();
    }

    /**
     * @param entries Entries to append.
     * @return The number of appended entries.
     */
    public long appendEntries(List<LogEntry> entries) {
        for (LogEntry entry : entries) {
            if (entry.index() > log.size() + 1)
                throw new UnrecoverableException("Invalid entry offset (a gap will be formed) [idx=" + entry.index() +
                    ", lastLogIdx=" + lastLogIndex() + ']');

            if (entry.index() <= log.size())
                truncate(entry.index());

            log.add(entry);
        }

        return entries.size();
    }

    /** {@inheritDoc} */
    @Override public long entryTerm(long idx) {
        if (idx == 0)
            return 0;

        return log.get((int)idx - 1).term();
    }

    /** {@inheritDoc} */
    @Override public int fetchEntries(long startIdx, LogEntry[] out) {
        int start = (int)startIdx - 1;

        int off = 0;

        for (int i = start; i < log.size() && off < out.length; i++, off++)
            out[off] = log.get(i);

        return off;
    }

    /** {@inheritDoc} */
    @Override public TermRange fetchTermRange(long logIdx) {
        if (log.isEmpty())
            return new TermRange(0, 0, 0);

        if (logIdx > log.size())
            logIdx = log.size();

        long term = log.get((int)logIdx - 1).term();
        int idx = (int)logIdx - 1;

        while (idx >= 0 && log.get(idx).term() == term)
            idx--;

        return new TermRange(idx + 2, logIdx, term);
    }

    /**
     * @param idx Index from which the log will be truncated, inclusive.
     */
    public void truncate(long idx) {
        log = new ArrayList<>(log.subList(0, (int)idx - 1));
    }
}
