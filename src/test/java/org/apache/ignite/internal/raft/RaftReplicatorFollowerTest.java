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

import java.util.UUID;
import org.apache.ignite.internal.raft.message.AppendEntriesResponse;
import org.apache.ignite.internal.raft.message.LogEntry;
import org.apache.ignite.internal.raft.message.SimulatedLogEntry;
import org.apache.ignite.internal.raft.message.VoteResponse;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.raft.ReplicatorTestUtils.EMPTY_TERMS;
import static org.apache.ignite.internal.raft.ReplicatorTestUtils.entriesFromTerms;
import static org.apache.ignite.internal.raft.ReplicatorTestUtils.environment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
// TODO add test to validate interaction with Raft Log when progress is not called.
public class RaftReplicatorFollowerTest {
    /**
     * Tests empty follower matches zero-based append entries request and then successfully appends an entry.
     */
    @Test
    public void testEmptyFollowerIndexMatchesEmptyLeader() {
        final long term = 11;

        // Index                   1
        // Leader
        // Follower
        StandaloneEnvironment env = environment().withGroup(2)
            .withPersistence(term, null, entriesFromTerms(EMPTY_TERMS))
            .buildFollower();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesRequest(
                node,
                term,
                0,
                0,
                RaftReplicator.EMPTY,
                0
            );

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            // Should be no changes to persist on follower.
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertEquals(term, res.term());
            assertTrue(res.success());
            assertEquals(0, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());
        }

        {
            env.emitAppendEntriesRequest(
                node,
                term,
                0,
                0,
                new LogEntry[] {
                    new SimulatedLogEntry(1, term, 1L)
                },
                0
            );

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            // Should be no changes in hard state.
            assertNull(p.hardState());
            assertEquals(1, p.logEntries().size());
            assertEquals((Integer)1, p.logEntries().get(0).data());
            assertEquals(term, p.logEntries().get(0).term());
            assertEquals(1L, p.logEntries().get(0).index());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();

            assertTrue(res.success());
            assertEquals(1, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());
        }
    }

    /**
     * Tests a non-empty follower replies with correct divergence bound and then appends an entry.
     */
    @Test
    public void testFollowerShiftsMatchIndex1() {
        final long term = 11;

        // Index                   1  2  3  4  5  6  7  8  9  10 11 12
        // Leader                  3  3  3  4, 4  4  6  6  6  8  8  8
        long[] terms = new long[] {3, 3, 3, 4, 4, 4, 6, 6, 6, 8, 8};

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            .withPersistence(term, null, ReplicatorTestUtils.entriesFromTerms(terms))
            .buildFollower();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesRequest(
                node,
                term,
                12,
                8,
                RaftReplicator.EMPTY,
                0
            );

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertFalse(res.success());
            assertEquals(0, res.matchIndex());
            assertEquals(10, res.conflictStart());
            assertEquals(11, res.conflictIndex());
            assertEquals(8, res.conflictTerm());

            assertEquals(11, env.persistence().lastLogIndex());
        }

        {
            env.emitAppendEntriesRequest(
                node,
                term,
                11,
                8,
                RaftReplicator.EMPTY,
                0
            );

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(11, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());
        }

        {
            env.emitAppendEntriesRequest(
                node,
                term,
                11,
                8,
                new LogEntry[] {
                    new SimulatedLogEntry(8, term, 12L)
                },
                0
            );

            Progress p = env.progress();
            assertNull(p.hardState());

            assertEquals(1, p.logEntries().size());
            assertEquals((Integer)8, p.logEntries().get(0).data());
            assertEquals(term, p.logEntries().get(0).term());
            assertEquals(12L, p.logEntries().get(0).index());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(12, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());
        }
    }

    /**
     */
    @Test
    public void testFollowerShiftsMatchIndex2() {
        final long term = 11;

        // Index                   1  2  3  4  5  6  7  8  9  10 11 12
        // Leader                  3  3  3  4  4  4  4  6  6  8  8  8
        long[] terms = new long[] {3, 3, 3, 4, 4, 4, 4, 7, 7, 7, 7};

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            .withPersistence(term, null, ReplicatorTestUtils.entriesFromTerms(terms))
            .buildFollower();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesRequest(node,
                term,
                12,
                8,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertFalse(res.success());
            assertEquals(0, res.matchIndex());
            assertEquals(8, res.conflictStart());
            assertEquals(11, res.conflictIndex());
            assertEquals(7, res.conflictTerm());

            assertEquals(11, env.persistence().lastLogIndex());
        }

        {
            env.emitAppendEntriesRequest(node,
                term,
                8,
                6,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertFalse(res.success());
            assertEquals(0, res.matchIndex());
            assertEquals(4, res.conflictStart());
            assertEquals(7, res.conflictIndex());
            assertEquals(4, res.conflictTerm());
        }

        {
            env.emitAppendEntriesRequest(node,
                term,
                7,
                4,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(7, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());
        }

        {
            LogEntry[] append = {
                new SimulatedLogEntry(8, 6, 8),
                new SimulatedLogEntry(9, 6, 9),
                new SimulatedLogEntry(10, 8, 10),
                new SimulatedLogEntry(11, 8, 11),
                new SimulatedLogEntry(12, 8, 12),
            };

            env.emitAppendEntriesRequest(node,
                term,
                7,
                4,
                append,
                0);

            Progress p = env.progress();
            assertNull(p.hardState());
            assertEquals(append.length, p.logEntries().size());
            for (int i = 0; i < append.length; i++)
                assertEquals(append[i], p.logEntries().get(i));

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(12, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());
        }
    }

    /**
     */
    @Test
    public void testFollowerShiftsMatchIndex3() {
        final long term = 11;

        // Index                   1  2  3  4  5  6  7  8  9  10 11 12 13
        // Leader                  3  3  4  4  6  6  6  6  6  8  8  8  8
        long[] terms = new long[] {3, 3, 3, 3, 3, 4, 4, 7, 7, 7, 7};

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            .withPersistence(term, null, ReplicatorTestUtils.entriesFromTerms(terms))
            .buildFollower();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesRequest(node,
                term,
                13,
                8,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertFalse(res.success());
            assertEquals(0, res.matchIndex());
            assertEquals(8, res.conflictStart());
            assertEquals(11, res.conflictIndex());
            assertEquals(7, res.conflictTerm());

            assertEquals(11, env.persistence().lastLogIndex());
        }

        {
            env.emitAppendEntriesRequest(node,
                term,
                8,
                6,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertFalse(res.success());
            assertEquals(0, res.matchIndex());
            assertEquals(6, res.conflictStart());
            assertEquals(7, res.conflictIndex());
            assertEquals(4, res.conflictTerm());
        }

        {
            env.emitAppendEntriesRequest(node,
                term,
                6,
                6,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertFalse(res.success());
            assertEquals(0, res.matchIndex());
            assertEquals(1, res.conflictStart());
            assertEquals(5, res.conflictIndex());
            assertEquals(3, res.conflictTerm());
        }

        {
            env.emitAppendEntriesRequest(node,
                term,
                3,
                4,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertFalse(res.success());
            assertEquals(0, res.matchIndex());
            assertEquals(1, res.conflictStart());
            assertEquals(2, res.conflictIndex());
            assertEquals(3, res.conflictTerm());
        }

        {
            env.emitAppendEntriesRequest(node,
                term,
                2,
                3,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(2, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());
        }

        {
            LogEntry[] append = {
                new SimulatedLogEntry(3, 4, 3),
                new SimulatedLogEntry(4, 4, 4),
                new SimulatedLogEntry(5, 6, 5),
                new SimulatedLogEntry(6, 6, 6),
                new SimulatedLogEntry(7, 6, 7),
                new SimulatedLogEntry(8, 6, 8),
                new SimulatedLogEntry(9, 6, 9),
                new SimulatedLogEntry(10, 8, 10),
                new SimulatedLogEntry(11, 8, 11),
                new SimulatedLogEntry(12, 8, 12),
                new SimulatedLogEntry(13, 8, 13),
            };

            env.emitAppendEntriesRequest(node,
                term,
                2,
                3,
                append,
                0);

            Progress p = env.progress();
            assertNull(p.hardState());
            assertEquals(append.length, p.logEntries().size());
            for (int i = 0; i < append.length; i++)
                assertEquals(append[i], p.logEntries().get(i));

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(13, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());

            assertEquals(13, env.persistence().lastLogIndex());
        }
    }

    /**
     */
    @Test
    public void testFollowerShiftsMatchIndex4() {
        final long term = 11;

        // Index                   1  2  3  4  5  6  7  8  9  10 11 12 13
        // Leader                  3  3  4  4  6  6  6  6  6  8  8  8  8
        long[] terms = new long[] {3, 3, 7, 7, 7, 7, 7, 7, 7, 7, 7};

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            .withPersistence(term, null, ReplicatorTestUtils.entriesFromTerms(terms))
            .buildFollower();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesRequest(node,
                term,
                13,
                8,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertFalse(res.success());
            assertEquals(0, res.matchIndex());
            assertEquals(3, res.conflictStart());
            assertEquals(11, res.conflictIndex());
            assertEquals(7, res.conflictTerm());
        }

        {
            env.emitAppendEntriesRequest(node,
                term,
                3,
                4,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertFalse(res.success());
            assertEquals(0, res.matchIndex());
            assertEquals(1, res.conflictStart());
            assertEquals(2, res.conflictIndex());
            assertEquals(3, res.conflictTerm());
        }

        {
            env.emitAppendEntriesRequest(node,
                term,
                2,
                3,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(2, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());
        }

        {
            LogEntry[] append = {
                new SimulatedLogEntry(3, 4, 3),
                new SimulatedLogEntry(4, 4, 4),
                new SimulatedLogEntry(5, 6, 5),
                new SimulatedLogEntry(6, 6, 6),
                new SimulatedLogEntry(7, 6, 7),
                new SimulatedLogEntry(8, 6, 8),
                new SimulatedLogEntry(9, 6, 9),
                new SimulatedLogEntry(10, 8, 10),
                new SimulatedLogEntry(11, 8, 11),
                new SimulatedLogEntry(12, 8, 12),
                new SimulatedLogEntry(13, 8, 13),
            };

            env.emitAppendEntriesRequest(node,
                term,
                2,
                3,
                append,
                0);

            Progress p = env.progress();
            assertNull(p.hardState());
            assertEquals(append.length, p.logEntries().size());
            for (int i = 0; i < append.length; i++)
                assertEquals(append[i], p.logEntries().get(i));

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(13, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());

            assertEquals(13, env.persistence().lastLogIndex());
        }
    }

    /**
     */
    @Test
    public void testFollowerShiftsMatchIndex5() {
        final long term = 11;

        // Index                   1  2  3
        // Leader                  3  3  3
        // Follower
        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            .withPersistence(term, null, ReplicatorTestUtils.entriesFromTerms(EMPTY_TERMS))
            .buildFollower();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesRequest(node,
                term,
                3,
                3,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertFalse(res.success());
            assertEquals(0, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());
        }

        {
            env.emitAppendEntriesRequest(node,
                term,
                0,
                0,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(0, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());
        }

        {
            LogEntry[] append = {
                new SimulatedLogEntry(1, 3, 1),
                new SimulatedLogEntry(2, 3, 2),
                new SimulatedLogEntry(3, 3, 3),
            };

            env.emitAppendEntriesRequest(node,
                term,
                0,
                0,
                append,
                0);

            Progress p = env.progress();
            assertNull(p.hardState());
            assertEquals(append.length, p.logEntries().size());
            for (int i = 0; i < append.length; i++)
                assertEquals(append[i], p.logEntries().get(i));

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(3, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());

            assertEquals(3, env.persistence().lastLogIndex());
        }
    }

    /**
     * TODO this looks identical to shift 5
     */
    @Test
    public void testFollowerShiftsMatchIndex6() {
        final long term = 11;

        // Index                   1  2  3  4  5
        // Leader                  3  3  3  4  4
        // Follower
        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            .withPersistence(term, null, ReplicatorTestUtils.entriesFromTerms(EMPTY_TERMS))
            .buildFollower();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesRequest(node,
                term,
                5,
                4,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertFalse(res.success());
            assertEquals(0, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());
        }

        {
            env.emitAppendEntriesRequest(node,
                term,
                0,
                0,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(0, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());

            assertEquals(0, env.persistence().lastLogIndex());
        }

        {
            LogEntry[] append = {
                new SimulatedLogEntry(1, 3, 1),
                new SimulatedLogEntry(2, 3, 2),
                new SimulatedLogEntry(3, 3, 3),
                new SimulatedLogEntry(4, 4, 4),
                new SimulatedLogEntry(5, 4, 5),
            };

            env.emitAppendEntriesRequest(node,
                term,
                0,
                0,
                append,
                0);

            Progress p = env.progress();
            assertNull(p.hardState());
            assertEquals(append.length, p.logEntries().size());
            for (int i = 0; i < append.length; i++)
                assertEquals(append[i], p.logEntries().get(i));

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(5, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());

            assertEquals(5, env.persistence().lastLogIndex());
        }
    }

    /**
     */
    @Test
    public void testFollowerShiftsMatchIndex7() {
        final long term = 11;

        // Index                   1  2  3  4  5
        // Leader                  3  3  3  4  4
        long[] terms = new long[] {3, 3, 5, 5, 5, 5, 6, 6};

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            .withPersistence(term, null, ReplicatorTestUtils.entriesFromTerms(terms))
            .buildFollower();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesRequest(node,
                term,
                5,
                4,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertFalse(res.success());
            assertEquals(0, res.matchIndex());
            assertEquals(3, res.conflictStart());
            assertEquals(4, res.conflictIndex());
            assertEquals(5, res.conflictTerm());
        }

        {
            env.emitAppendEntriesRequest(node,
                term,
                3,
                3,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertFalse(res.success());
            assertEquals(0, res.matchIndex());
            assertEquals(1, res.conflictStart());
            assertEquals(2, res.conflictIndex());
            assertEquals(3, res.conflictTerm());
        }

        {
            env.emitAppendEntriesRequest(node,
                term,
                2,
                3,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(2, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());
        }

        {
            LogEntry[] append = {
                new SimulatedLogEntry(3, 3, 3),
                new SimulatedLogEntry(4, 4, 4),
                new SimulatedLogEntry(5, 4, 5),
            };

            env.emitAppendEntriesRequest(node,
                term,
                2,
                3,
                append,
                0);

            Progress p = env.progress();
            assertNull(p.hardState());
            assertEquals(append.length, p.logEntries().size());
            for (int i = 0; i < append.length; i++)
                assertEquals(append[i], p.logEntries().get(i));

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(5, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());

            assertEquals(5, env.persistence().lastLogIndex());
        }
    }

    /**
     */
    @Test
    public void testFollowerIgnoresDuplicatedAppendEntries() {
        final long term = 11;

        // Index                   1  2  3  4  5
        // Leader                  3  3  3  3  3
        long[] terms = new long[] {3, 3, 3};

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            .withPersistence(term, null, ReplicatorTestUtils.entriesFromTerms(terms))
            .buildFollower();

        UUID node = env.remotePeer();

        {
            LogEntry[] append = {
                new SimulatedLogEntry(4, 3, 4),
            };

            env.emitAppendEntriesRequest(node,
                term,
                3,
                3,
                append,
                0);

            Progress p = env.progress();
            assertNull(p.hardState());
            assertEquals(append.length, p.logEntries().size());
            for (int i = 0; i < append.length; i++)
                assertEquals(append[i], p.logEntries().get(i));

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(4, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());

            assertEquals(4, env.persistence().lastLogIndex());
        }

        {
            LogEntry[] append = {
                new SimulatedLogEntry(5, 3, 5),
            };

            env.emitAppendEntriesRequest(node,
                term,
                4,
                3,
                append,
                0);

            Progress p = env.progress();
            assertNull(p.hardState());
            assertEquals(append.length, p.logEntries().size());
            for (int i = 0; i < append.length; i++)
                assertEquals(append[i], p.logEntries().get(i));

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(5, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());

            assertEquals(5, env.persistence().lastLogIndex());
        }

        {
            LogEntry[] append = {
                new SimulatedLogEntry(4, 3, 4),
            };

            env.emitAppendEntriesRequest(node,
                term,
                3,
                3,
                append,
                0);

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof AppendEntriesResponse);

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(5, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());
        }
    }

    // TODO RAFT-4
//    @Test
//    public void testFollowerPersistencePartialWrite() {
//        final long term = 11;
//
//        // Index                   1  2  3
//        // Leader                  3  3  3
//        long[] terms = new long[] {3, 3, 3};
//
//        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
//            .withPersistence(2, term, null, ReplicatorTestUtils.entries(terms))
//            .buildFollower();
//
//        RaftNode node = env.replicationGroup().remoteNodes().get(0);
//
//        {
//            env.emitAppendEntriesRequest(node,
//                term,
//                3,
//                3,
//                new LogEntry[] {
//                    new SimulatedLogEntry(3, new Noop()),
//                    new SimulatedLogEntry(3, new Noop()),
//                    new SimulatedLogEntry(3, new Noop()),
//                },
//                0);
//
//            AppendEntriesResponse res = (AppendEntriesResponse)env.consumeMessages(node)
//                .collect(single(m -> m instanceof AppendEntriesResponse && m.term() == term));
//
//            assertTrue(res.success());
//            assertEquals(5, res.matchIndex());
//            assertEquals(0, res.conflictStart());
//            assertEquals(0, res.conflictIndex());
//            assertEquals(0, res.conflictTerm());
//
//            assertEquals(5, env.persistence().lastLogIndex());
//        }
//    }

    /**
     */
    @Test
    public void testFollowerUpgradesTermOnGreaterTermVoteRequest() {
        final long term = 11;

        long[] terms = new long[] {3, 3, 3};

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            .withPersistence(term - 1, null, ReplicatorTestUtils.entriesFromTerms(terms))
            .buildFollower();

        UUID node = env.remotePeer();

        {
            assertEquals(term - 1, env.persistence().hardState().term());

            env.emitVoteRequest(node,
                term,
                3,
                3);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNotNull(p.hardState());
            assertEquals(term, p.hardState().term());
            assertEquals(node, p.hardState().votedFor());

            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());
            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());
            assertTrue(msg.message() instanceof VoteResponse);

            VoteResponse res = msg.message();

            assertTrue(res.voteGranted());
        }
    }

    /**
     */
    @Test
    public void testFollowerUpgradesTermOnGreaterTermAppendEntriesRequest() {
        final long term = 11;

        long[] terms = new long[] {3, 3, 3};

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            .withPersistence(term - 1, UUID.randomUUID(), ReplicatorTestUtils.entriesFromTerms(terms))
            .buildFollower();

        UUID node = env.remotePeer();

        {
            assertEquals(term - 1, env.persistence().hardState().term());

            env.emitAppendEntriesRequest(node,
                term,
                3,
                3,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNotNull(p.hardState());
            assertEquals(term, p.hardState().term());
            assertNull(p.hardState().votedFor());

            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());
            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(3, res.matchIndex());

            assertEquals(3, env.persistence().lastLogIndex());
        }
    }

    /**
     */
    @Test
    public void testFollowerDoesNotCampaingWhenHeartbeatsAreReceived() {
        final long term = 11;

        long[] terms = new long[] {3, 3, 3};

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            .withPersistence(term, UUID.randomUUID(), ReplicatorTestUtils.entriesFromTerms(terms))
            .withElectionTimeout(2000) // On start, first election is in range [0.5, 1] * election timeout.
            .buildFollower();

        UUID node = env.remotePeer();

        {
            env.step(999);
            assertFalse(env.hasProgress());

            env.emitAppendEntriesRequest(node,
                term,
                3,
                3,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();

            assertEquals(ReplicatorRole.FOLLOWER, p.role());
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());
            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(3, res.matchIndex());

            env.step(1);
            assertEquals(ReplicatorRole.FOLLOWER, env.replicator().role());
            assertFalse(env.hasProgress());
        }

        {
            env.step(1999);
            assertFalse(env.hasProgress());

            env.emitAppendEntriesRequest(node,
                term,
                3,
                3,
                RaftReplicator.EMPTY,
                0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();

            assertEquals(ReplicatorRole.FOLLOWER, p.role());
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());
            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());

            AppendEntriesResponse res = msg.message();
            assertTrue(res.success());
            assertEquals(3, res.matchIndex());

            env.step(1);
            assertEquals(ReplicatorRole.FOLLOWER, env.replicator().role());
            assertFalse(env.hasProgress());
        }
    }
}