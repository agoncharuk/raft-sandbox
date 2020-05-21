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

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.internal.raft.message.LogEntry;
import org.apache.ignite.internal.raft.message.SimulatedMessageFactory;
import org.apache.ignite.internal.raft.service.ConfigurationState;
import org.apache.ignite.internal.raft.service.HardState;
import org.apache.ignite.internal.raft.service.TermRange;
import org.apache.ignite.internal.raft.service.PersistenceService;
import org.apache.ignite.internal.raft.service.SimulatedPersistenceService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TODO: fetchEntries after truncate
 * TODO: fetchTerm after truncate
 * TODO: appendEntries after truncate
 * TODO: progress, with and without truncate
 */
public class RaftLogTest {
    /**
     */
    public static Stream<Arguments> fetchEntriesArguments() {
        return Stream.of(
            Arguments.of(0, 5, 2),
            Arguments.of(1, 5, 2),
            Arguments.of(5, 0, 2),
            Arguments.of(5, 1, 2),

            Arguments.of(0, 5, 1),
            Arguments.of(1, 5, 1),
            Arguments.of(5, 0, 1),
            Arguments.of(5, 1, 1),

            Arguments.of(0, 5, 5),
            Arguments.of(1, 5, 6),
            Arguments.of(5, 0, 5),
            Arguments.of(5, 1, 6),

            Arguments.of(0, 5, 10),
            Arguments.of(1, 5, 10),
            Arguments.of(5, 0, 10),
            Arguments.of(5, 1, 10)
        );
    }

    /**
     * @param persistenceSize Number of entries to put to stable storage.
     * @param unstableSize Number of entries to put to volatile storage.
     * @param windowSize Window size to fetch entries.
     */
    @ParameterizedTest
    @MethodSource("fetchEntriesArguments")
    public void testFetchEntriesMixed(
        int persistenceSize,
        int unstableSize,
        int windowSize
    ) {
        long[] persistence = new long[persistenceSize];
        for (int i = 0; i < persistenceSize; i++)
            persistence[i] = i + 1;

        long[] unstable = new long[unstableSize];
        for (int i = 0; i < unstableSize; i++)
            unstable[i] = persistenceSize + i + 1;

        RaftLog log = buildLogFromIndexes(persistence, unstable);

        for (int p = 0; p < persistenceSize + unstableSize; p++) {
            LogEntry[] logEntries = log.fetchEntries(p + 1, windowSize);

            int shouldFetch = Math.min(windowSize, persistenceSize + unstableSize - p);

            assertEquals(shouldFetch, logEntries.length);

            for (int i = 0; i < logEntries.length; i++) {
                assertEquals(p + i + 1, logEntries[i].index());
                assertEquals(p + i + 1, logEntries[i].term());
                assertEquals(p + i + 1, (Long)(logEntries[i].data()));
            }
        }
    }

    /**
     */
    @Test
    public void testEntryTerm() {
        RaftLog log = buildLogFromTerms(new long[] {1, 2, 3, 4, 5}, new long[] {6, 7, 8, 9, 10});

        for (int i = 0; i <= log.lastLogIndex(); i++)
            assertEquals(i, log.entryTerm(i));
    }

    /**
     * Log structure is encoded in the test as map from term start index -> term. Last entry in the map represents
     * a single entry with the given index.
     * The test generates such a log and splits it into stable and unstable parts around the given point.
     * Later these parts are used to construct raft log and query term start for each possible index.
     *
     * @param split Split point.
     */
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})
    public void testFetchTerm(int split) {
        final NavigableMap<Long, Long> termIdx = new TreeMap<Long, Long>() {
            {
                put(1L, 1L);
                put(2L, 2L);
                put(3L, 3L);
                put(5L, 4L);
                put(8L, 5L);
                put(13L, 6L);
            }
        };

        int lastLogIdx = termIdx.lastEntry().getKey().intValue();

        assertTrue(split <= lastLogIdx);

        // Unfold index stable and unstable storage.
        long[] stable = new long[split];
        long[] unstable = new long[lastLogIdx - split];

        for (int i = 0; i < lastLogIdx; i++) {
            long term = termIdx.floorEntry((long)(i + 1)).getValue();

            if (i < split)
                stable[i] = term;
            else
                unstable[i - split] = term;
        }

        RaftLog log = buildLogFromTerms(stable, unstable);

        for (int i = 1; i <= lastLogIdx; i++) {
            TermRange range = log.fetchTermStart(i);

            assertNotNull(range);

            // Look up the actual term start.
            Map.Entry<Long, Long> entry = termIdx.floorEntry((long)i);

            assertEquals(entry.getKey(), range.startIndex());
            assertEquals(i, range.endIndex());
            assertEquals(entry.getValue(), range.term());
        }
    }

    /**
     */
    private RaftLog buildLogFromIndexes(long[] persistence, long[] unstable) {
        UUID[] g = new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};

        PersistenceService pSvc = new SimulatedPersistenceService(
            new HardState(100, null, 0),
            new ConfigurationState(g[0], g),
            ReplicatorTestUtils.entriesFromIndexes(persistence));

        RaftLog log = new RaftLog(pSvc, new SimulatedMessageFactory());

        log.addUnstable(ReplicatorTestUtils.entriesFromIndexes(unstable));

        return log;
    }

    /**
     */
    private RaftLog buildLogFromTerms(long[] persistence, long[] unstable) {
        UUID[] g = new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};

        PersistenceService pSvc = new SimulatedPersistenceService(
            new HardState(100, null, 0),
            new ConfigurationState(g[0], g),
            ReplicatorTestUtils.entriesFromTermsWithOffset(0, persistence));

        RaftLog log = new RaftLog(pSvc, new SimulatedMessageFactory());

        log.addUnstable(ReplicatorTestUtils.entriesFromTermsWithOffset(persistence.length, unstable));

        return log;
    }
}
