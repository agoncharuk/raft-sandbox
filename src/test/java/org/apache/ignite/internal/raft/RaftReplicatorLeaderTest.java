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

import java.util.Iterator;
import java.util.UUID;
import org.apache.ignite.internal.raft.message.AppendEntriesRequest;
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 */
public class RaftReplicatorLeaderTest {
    /**
     */
    @Test
    public void testLeaderConvertsToFollowerOnGreaterTermVoteRequest() {
        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(3).buildLeader();

        long term = env.persistence().hardState().term() + 1;

        UUID requester = env.remotePeer();

        env.emitVoteRequest(
            requester,
            term,
            0,
            0
        );

        assertTrue(env.hasProgress());

        Progress p = env.progress();
        assertEquals(ReplicatorRole.FOLLOWER, p.role());
        assertNotNull(p.hardState());
        assertEquals(term, p.hardState().term());
        assertEquals(requester, p.hardState().votedFor());

        assertNull(p.logEntries());

        assertEquals(1, p.messages().size());
        OutgoingMessage msg = p.messages().get(0);
        assertEquals(requester, msg.destinationPeerId());

        VoteResponse res = msg.message();

        assertTrue(res.voteGranted());
    }

    /**
     */
    @Test
    public void testEmptyLeaderIndexMatchesEmptyFollower() {
        final long term = 10;

        // Index                   1
        // Follower
        // Leader
        StandaloneEnvironment env = environment().withGroup(2)
            // Term will be incremented during election.
            .withPersistence(term - 1, null, entriesFromTerms(EMPTY_TERMS))
            .buildLeader();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                0,
                0,
                0,
                0,
                0
            );

            assertFalse(env.hasProgress());
        }
    }

    /**
     */
    @Test
    public void testLeaderShiftsMatchIndex1() {
        final long term = 10;

        // Index                   1  2  3  4  5  6  7  8  9  10 11 12
        // Follower                3  3  3  4  4  4  6  6  6  8  8
        long[] terms = new long[] {3, 3, 3, 4, 4, 4, 6, 6, 6, 8, 8, 8};

        LogEntry[] entries = entriesFromTerms(terms);

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            // Term will be incremented during election.
            .withPersistence(term - 1, null, entries)
            .buildLeader();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                false,
                0,
                0,
                10,
                11,
                8
            );

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(term, req.term());
            assertEquals(11, req.lastLogIndex());
            assertEquals(8, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);
        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                11,
                0,
                0,
                0,
                0
            );

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(term, req.term());
            assertEquals(11, req.lastLogIndex());
            assertEquals(8, req.lastLogTerm());
            assertEquals(1, req.logEntries().length);
            assertEquals(entries[entries.length - 1], req.logEntries()[0]);
        }

        assertFalse(env.hasProgress());
    }

    /**
     */
    @Test
    public void testLeaderShiftsMatchIndex2() {
        final long term = 10;

        // Index                   1  2  3  4  5  6  7  8  9  10 11 12
        // Follower                3  3  3  4  4  4  4  7  7  7  7
        long[] terms = new long[] {3, 3, 3, 4, 4, 4, 4, 6, 6, 8, 8, 8};

        LogEntry[] entries = entriesFromTerms(terms);

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            // Term will be incremented during election.
            .withPersistence(term - 1, null, entries)
            .buildLeader();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                false,
                0,
                0,
                8,
                11,
                7
            );

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(8, req.lastLogIndex());
            assertEquals(6, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);
        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                false,
                0,
                0,
                4,
                7,
                4
            );

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(7, req.lastLogIndex());
            assertEquals(4, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);
        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                7,
                0,
                0,
                0,
                0
            );

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(7, req.lastLogIndex());
            assertEquals(4, req.lastLogTerm());
            assertEquals(5, req.logEntries().length);

            for (int i = 0; i < req.logEntries().length; i++)
                assertEquals(entries[(int)req.lastLogIndex() + i], req.logEntries()[i]);
        }

        assertFalse(env.hasProgress());
    }

    /**
     */
    @Test
    public void testLeaderShiftsMatchIndex3() {
        final long term = 10;

        // Index                   1  2  3  4  5  6  7  8  9  10 11 12 13
        // Follower                3  3  3  3  3  4  4  7  7  7  7
        long[] terms = new long[] {3, 3, 4, 4, 6, 6, 6, 6, 6, 8, 8, 8, 8};

        LogEntry[] entries = entriesFromTerms(terms);

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            // Term will be incremented during election.
            .withPersistence(term - 1, null, entries)
            .buildLeader();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                false,
                0,
                0,
                8,
                11,
                7
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(8, req.lastLogIndex());
            assertEquals(6, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);
        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                false,
                0,
                0,
                6,
                7,
                4
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(6, req.lastLogIndex());
            assertEquals(6, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);
        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                false,
                0,
                0,
                1,
                5,
                3
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(3, req.lastLogIndex());
            assertEquals(4, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);
        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                false,
                0,
                0,
                1,
                2,
                3
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(2, req.lastLogIndex());
            assertEquals(3, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);
        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                2,
                0,
                0,
                0,
                0
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(2, req.lastLogIndex());
            assertEquals(3, req.lastLogTerm());
            assertEquals(11, req.logEntries().length);
            for (int i = 0; i < req.logEntries().length; i++)
                assertEquals(entries[(int)req.lastLogIndex() + i], req.logEntries()[i]);
        }

        assertFalse(env.hasProgress());
    }

    /**
     */
    @Test
    public void testLeaderShiftsMatchIndex4() {
        final long term = 10;

        // Index                   1  2  3  4  5  6  7  8  9  10 11 12 13
        // Follower                3  3  7  7  7  7  7  7  7  7  7
        long[] terms = new long[] {3, 3, 4, 4, 6, 6, 6, 6, 6, 8, 8, 8, 8};

        LogEntry[] entries = entriesFromTerms(terms);

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            // Term will be incremented during election.
            .withPersistence(term - 1, null, entries)
            .buildLeader();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                false,
                0,
                0,
                3,
                11,
                7
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(3, req.lastLogIndex());
            assertEquals(4, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);
        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                false,
                0,
                0,
                1,
                2,
                3
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(2, req.lastLogIndex());
            assertEquals(3, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);
        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                2,
                0,
                0,
                0,
                0
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(2, req.lastLogIndex());
            assertEquals(3, req.lastLogTerm());
            assertEquals(11, req.logEntries().length);
            for (int i = 0; i < req.logEntries().length; i++)
                assertEquals(entries[(int)req.lastLogIndex() + i], req.logEntries()[i]);
        }

        assertFalse(env.hasProgress());
    }

    /**
     */
    @Test
    public void testLeaderShiftsMatchIndex5() {
        final long term = 10;

        // Index                   1  2  3
        // Follower
        long[] terms = new long[] {3, 3, 3};

        LogEntry[] entries = entriesFromTerms(terms);

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            // Term will be incremented during election.
            .withPersistence(term - 1, null, entries)
            .buildLeader();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                false,
                0,
                0,
                0,
                0,
                0
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(0, req.lastLogIndex());
            assertEquals(0, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);
        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                0,
                0,
                0,
                0,
                0
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(0, req.lastLogIndex());
            assertEquals(0, req.lastLogTerm());
            assertEquals(3, req.logEntries().length);
            for (int i = 0; i < req.logEntries().length; i++)
                assertEquals(entries[(int)req.lastLogIndex() + i], req.logEntries()[i]);
        }

        assertFalse(env.hasProgress());
    }

    /**
     */
    @Test
    public void testLeaderShiftsMatchIndex6() {
        final long term = 10;

        // Index                   1  2  3  4  5
        // Follower
        long[] terms = new long[] {3, 3, 3, 4, 4};

        LogEntry[] entries = entriesFromTerms(terms);

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            // Term will be incremented during election.
            .withPersistence(term - 1, null, entries)
            .buildLeader();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                false,
                0,
                0,
                0,
                0,
                0
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(0, req.lastLogIndex());
            assertEquals(0, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);
        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                0,
                0,
                0,
                0,
                0
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(0, req.lastLogIndex());
            assertEquals(0, req.lastLogTerm());
            assertEquals(5, req.logEntries().length);
            for (int i = 0; i < req.logEntries().length; i++)
                assertEquals(entries[(int)req.lastLogIndex() + i], req.logEntries()[i]);
        }

        assertFalse(env.hasProgress());
    }

    /**
     */
    @Test
    public void testLeaderShiftsMatchIndex7() {
        final long term = 10;

        // Index                   1  2  3  4  5
        // Follower                3  3  5  5  5  5  6  6
        long[] terms = new long[] {3, 3, 3, 4, 4};

        LogEntry[] entries = entriesFromTerms(terms);

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            // Term will be incremented during election.
            .withPersistence(term - 1, null, entries)
            .buildLeader();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                false,
                0,
                0,
                3,
                4,
                5
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(3, req.lastLogIndex());
            assertEquals(3, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);
        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                false,
                0,
                0,
                1,
                2,
                3
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(2, req.lastLogIndex());
            assertEquals(3, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);
        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                2,
                0,
                0,
                0,
                0
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(2, req.lastLogIndex());
            assertEquals(3, req.lastLogTerm());
            assertEquals(3, req.logEntries().length);
            for (int i = 0; i < req.logEntries().length; i++)
                assertEquals(entries[(int)req.lastLogIndex() + i], req.logEntries()[i]);
        }

        assertFalse(env.hasProgress());
    }

    /**
     */
    @Test
    public void testLeaderAppendsEntries() {
        final long term = 10;

        // Index                   1  2  3  4  5
        // Follower
        long[] terms = new long[] {3, 3, 3, 3, 3};

        LogEntry[] entries = entriesFromTerms(terms);

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            // Term will be incremented during election.
            .withPersistence(term - 1, null, entries)
            .withReplicatorConfig(
                new ReplicatorConfig()
                    .setElectionTimeout(1000)
                    .setAppendEntriesBatchSize(2))
            .buildLeader();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                false,
                0,
                0,
                0,
                0,
                0
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(0, req.lastLogIndex());
            assertEquals(0, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);
        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                0,
                0,
                0,
                0,
                0
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(0, req.lastLogIndex());
            assertEquals(0, req.lastLogTerm());
            assertEquals(2, req.logEntries().length);
            for (int i = 0; i < req.logEntries().length; i++)
                assertEquals(entries[(int)req.lastLogIndex() + i], req.logEntries()[i]);
        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                2,
                0,
                0,
                0,
                0
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(2, req.lastLogIndex());
            assertEquals(3, req.lastLogTerm());
            assertEquals(2, req.logEntries().length);
            for (int i = 0; i < req.logEntries().length; i++)
                assertEquals(entries[(int)req.lastLogIndex() + i], req.logEntries()[i]);
        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                4,
                0,
                0,
                0,
                0
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(0, req.commitIndex());
            assertEquals(4, req.lastLogIndex());
            assertEquals(3, req.lastLogTerm());
            assertEquals(1, req.logEntries().length);
            for (int i = 0; i < req.logEntries().length; i++)
                assertEquals(entries[(int)req.lastLogIndex() + i], req.logEntries()[i]);

        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                5,
                0,
                0,
                0,
                0
            );
        }

        assertFalse(env.hasProgress());
    }

    /**
     */
    @Test
    public void testLeaderResendsAppendEntriesRequestOnResponseTimeout() {
        final long term = 10;

        // Index                   1  2  3  4  5
        // Follower
        long[] terms = new long[] {3, 3, 3, 3, 3};

        LogEntry[] entries = entriesFromTerms(terms);

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            // Term will be incremented during election.
            .withPersistence(term - 1, null, entries)
            .withReplicatorConfig(
                new ReplicatorConfig()
                    .setElectionTimeout(3000))
            .buildLeader();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                false,
                0,
                0,
                0,
                0,
                0
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(0, req.lastLogIndex());
            assertEquals(0, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);
        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                0,
                0,
                0,
                0,
                0
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(0, req.lastLogIndex());
            assertEquals(0, req.lastLogTerm());
            assertEquals(5, req.logEntries().length);
            for (int i = 0; i < req.logEntries().length; i++)
                assertEquals(entries[(int)req.lastLogIndex() + i], req.logEntries()[i]);
        }

        {
            env.step(500);

            assertFalse(env.hasProgress());

            env.step(500);

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(0, req.lastLogIndex());
            assertEquals(0, req.lastLogTerm());
            assertEquals(5, req.logEntries().length);
            for (int i = 0; i < req.logEntries().length; i++)
                assertEquals(entries[(int)req.lastLogIndex() + i], req.logEntries()[i]);
        }
    }

    /**
     */
    @Test
    public void testLeaderSendsHeartbeat() {
        final long term = 10;

        // Index                   1  2  3  4  5
        // Follower                3  3  3  3  3
        long[] terms = new long[] {3, 3, 3, 3, 3};

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            // Term will be incremented during election.
            .withPersistence(term - 1, null, ReplicatorTestUtils.entriesFromTerms(terms))
            .withReplicatorConfig(
                new ReplicatorConfig()
                    .setElectionTimeout(4000))
            .buildLeader();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                5,
                0,
                0,
                0,
                0
            );

            assertFalse(env.hasProgress());
        }

        {
            env.step(500);
            assertFalse(env.hasProgress());

            env.step(500);

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(5, req.lastLogIndex());
            assertEquals(3, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);

            env.step(500);
            assertFalse(env.hasProgress());
        }
    }

    /**
     */
    @Test
    public void testLeaderSendsAppendEntriesRequestAfterHeartbeat() {
        final long term = 10;

        // Index                   1  2  3  4  5
        // Follower                3  3  3  3  3
        long[] terms = new long[] {3, 3, 3, 3, 3};

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            // Term will be incremented during election.
            .withPersistence(term - 1, null, ReplicatorTestUtils.entriesFromTerms(terms))
            .withReplicatorConfig(
                new ReplicatorConfig()
                    .setElectionTimeout(3000))
            .buildLeader();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                5,
                0,
                0,
                0,
                0
            );

            assertFalse(env.hasProgress());
        }

        {
            env.step(500);
            assertFalse(env.hasProgress());

            env.step(500);

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(5, req.lastLogIndex());
            assertEquals(3, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);

            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                5,
                0,
                0,
                0,
                0
            );

            assertFalse(env.hasProgress());
        }

        {
            env.replicator().appendEntries(6);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertEquals(1, p.logEntries().size());
            assertEquals(new SimulatedLogEntry(6, term, 6), p.logEntries().get(0));

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(5, req.lastLogIndex());
            assertEquals(3, req.lastLogTerm());
            assertEquals(1, req.logEntries().length);
            assertEquals(p.logEntries().get(0), req.logEntries()[0]);
        }
        {
            // While there is an outstanding request, no further requests should be issued (at least for now).
            env.replicator().appendEntries(7);

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertEquals(1, p.logEntries().size());
            assertEquals(new SimulatedLogEntry(7, term, 7), p.logEntries().get(0));

            assertNull(p.messages());
        }
    }

    /**
     */
    @Test
    public void testEmptyLeaderSendsAppendEntriesRequestAfterHeartbeat() {
        final long term = 10;

        // Index                   1  2  3  4  5
        // Follower
        // Leader
        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            // Term will be incremented during election.
            .withPersistence(term - 1, null, ReplicatorTestUtils.entriesFromTerms(EMPTY_TERMS))
            .withReplicatorConfig(
                new ReplicatorConfig()
                    .setElectionTimeout(3000))
            .buildLeader();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                0,
                0,
                0,
                0,
                0
            );

            assertFalse(env.hasProgress());
        }

        {
            env.step(500);
            assertFalse(env.hasProgress());

            env.step(500);
            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(0, req.lastLogIndex());
            assertEquals(0, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);

            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                0,
                0,
                0,
                0,
                0
            );

            assertFalse(env.hasProgress());
        }

        {
            SimulatedLogEntry entry = new SimulatedLogEntry(1, term, 1);
            env.replicator().appendEntries(1);
            assertTrue(env.hasProgress());

            Progress p = env.progress();

            assertNull(p.hardState());
            assertEquals(1, p.logEntries().size());
            assertEquals(entry, p.logEntries().get(0));

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(0, req.lastLogIndex());
            assertEquals(0, req.lastLogTerm());
            assertEquals(1, req.logEntries().length);
            assertEquals(entry, req.logEntries()[0]);
        }
    }

    /**
     */
    @Test
    public void testLeaderConvertsToFollowerOnGreaterTermAppendEntriesRequest() {
        final long term = 10;

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            // Term will be incremented during election.
            .withPersistence(term - 1, null)
            .withReplicatorConfig(
                new ReplicatorConfig()
                    .setElectionTimeout(3000))
            .buildLeader();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesRequest(
                node,
                term + 1,
                0,
                0,
                RaftReplicator.EMPTY,
                0
            );

            assertEquals(ReplicatorRole.FOLLOWER, env.replicator().role());

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertEquals(ReplicatorRole.FOLLOWER, p.role());

            assertEquals(term + 1, p.hardState().term());
            assertNull(p.hardState().votedFor());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());

            AppendEntriesResponse res = msg.message();
            assertEquals(term + 1, res.term());
            assertTrue(res.success());
            assertEquals(0, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());
        }
    }

    /**
     */
    @Test
    public void testLeaderAppendsEntriesAfterConversionToFollower() {
        final long term = 10;

        final long terms[] = new long[] {3, 3, 3};

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            // Term will be incremented during election.
            .withPersistence(term - 1, null, entriesFromTerms(terms))
            .withReplicatorConfig(
                new ReplicatorConfig()
                    .setElectionTimeout(3000))
            .buildLeader();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesRequest(
                node,
                term + 1,
                3,
                3,
                RaftReplicator.EMPTY,
                0
            );

            assertTrue(env.hasProgress());

            Progress p = env.progress();

            assertEquals(term + 1, p.hardState().term());
            assertNull(p.hardState().votedFor());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());

            AppendEntriesResponse res = msg.message();
            assertEquals(term + 1, res.term());
            assertTrue(res.success());
            assertEquals(3, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());
        }

        {
            LogEntry[] entries = {
                new SimulatedLogEntry(4, 4, 4)
            };

            env.emitAppendEntriesRequest(
                node,
                term + 1,
                3,
                3,
                entries,
                0
            );

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNull(p.hardState());

            assertEquals(1, p.logEntries().size());
            assertEquals(entries[0], p.logEntries().get(0));

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());

            AppendEntriesResponse res = msg.message();
            assertEquals(term + 1, res.term());
            assertTrue(res.success());
            assertEquals(4, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());
        }
    }

    /**
     */
    @Test
    public void testLeaderConvertsToFollowerOnGreaterTermAppendEntriesResponse() {
        final long term = 10;

        final long terms[] = new long[] {3, 3, 3};

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            // Term will be incremented during election.
            .withPersistence(term - 1, null, entriesFromTerms(terms))
            .withReplicatorConfig(
                new ReplicatorConfig()
                    .setElectionTimeout(3000))
            .buildLeader();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesResponse(node,
                term + 1,
                false,
                0,
                0,
                0,
                0,
                0);

            assertEquals(ReplicatorRole.FOLLOWER, env.replicator().role());

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertEquals(ReplicatorRole.FOLLOWER, p.role());
            assertNotNull(p.hardState());
            assertEquals(term + 1, p.hardState().term());
            assertNull(p.hardState().votedFor());

            assertNull(p.logEntries());
            assertNull(p.messages());
        }
    }

    /**
     */
    @Test
    public void testLeaderRespondsToStaleAppendEntriesRequest() {
        final long term = 10;

        final long terms[] = new long[] {3, 3, 3};

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            // Term will be incremented during election.
            .withPersistence(term - 1, null, entriesFromTerms(terms))
            .withReplicatorConfig(
                new ReplicatorConfig()
                    .setElectionTimeout(3000))
            .buildLeader();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesRequest(node,
                term - 1,
                2,
                3,
                RaftReplicator.EMPTY,
                0);

            assertEquals(ReplicatorRole.LEADER, env.replicator().role());

            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertEquals(ReplicatorRole.LEADER, p.role());

            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());

            AppendEntriesResponse res = msg.message();
            assertEquals(term, res.term());
            assertFalse(res.success());
            assertEquals(0, res.matchIndex());
            assertEquals(0, res.conflictStart());
            assertEquals(0, res.conflictIndex());
            assertEquals(0, res.conflictTerm());
        }
    }

    /**
     */
    @Test
    public void testLeaderAdvancesCommitIndexTwoNodes() {
        final long term = 10;

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            // Term will be incremented during election.
            .withPersistence(term - 1, null)
            .withReplicatorConfig(
                new ReplicatorConfig()
                    .setElectionTimeout(3000))
            .buildLeader();

        UUID node = env.remotePeer();

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                0,
                0,
                0,
                0,
                0
            );

            assertFalse(env.hasProgress());

            env.replicator().appendEntries(1);
            LogEntry[] entries = entriesFromTerms(term);

            assertTrue(env.hasProgress());
            Progress p = env.progress();

            assertNull(p.hardState());
            assertEquals(1, p.logEntries().size());
            assertEquals(entries[0], p.logEntries().get(0));

            assertEquals(1, p.messages().size());
            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(term, req.term());
            assertEquals(0, req.commitIndex());
            assertEquals(0, req.lastLogIndex());
            assertEquals(0, req.lastLogTerm());
            assertEquals(1, req.logEntries().length);
        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                1,
                0,
                0,
                0,
                0
            );

            assertTrue(env.hasProgress());

            Progress p = env.progress();

            assertNotNull(p.hardState());
            assertEquals(1, p.hardState().commitIndex());
            assertEquals(term, p.hardState().term());

            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());
            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(term, req.term());
            assertEquals(1, req.commitIndex());
            assertEquals(1, req.lastLogIndex());
            assertEquals(term, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);
        }

        {
            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                1,
                1,
                0,
                0,
                0
            );

            assertFalse(env.hasProgress());
        }
    }

    /**
     */
    @Test
    public void testFetchesEntriesFromStableAndUnstable() {
        long term = 10;
        // Index                   1  2  3  4  5
        // Follower
        long[] terms = new long[] {3, 3};

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(2)
            // Term will be incremented during election.
            .withPersistence(term - 1, null, ReplicatorTestUtils.entriesFromTerms(terms))
            .withReplicatorConfig(
                new ReplicatorConfig()
                    .setElectionTimeout(3000))
            .buildLeader();

        UUID node = env.remotePeer();

        {
            assertFalse(env.hasProgress());
            assertEquals(term, env.persistence().hardState().term());

            env.emitAppendEntriesResponse(
                node,
                term,
                false,
                0,
                0,
                0,
                0,
                0
            );

            Progress p = env.progress();
            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());

            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(0, req.lastLogIndex());
            assertEquals(0, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);
        }

        {
            env.replicator().appendEntries(4);

            assertTrue(env.hasProgress());

            env.emitAppendEntriesResponse(
                node,
                term,
                true,
                0,
                0,
                0,
                0,
                0
            );

            Progress p = env.progress();

            assertNull(p.hardState());
            assertEquals(1, p.logEntries().size());

            assertEquals(1, p.messages().size());
            OutgoingMessage msg = p.messages().get(0);

            assertEquals(node, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(0, req.lastLogIndex());
            assertEquals(0, req.lastLogTerm());
            assertEquals(3, req.logEntries().length);
            assertEquals(new SimulatedLogEntry(4, term, 3), req.logEntries()[2]);
        }
    }

    /**
     */
    @Test
    public void testLeaderAdvancesCommitIndexThreeNodes() {
        final long term = 10;

        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(3)
            // Term will be incremented during election.
            .withPersistence(term - 1, null)
            .withReplicatorConfig(
                new ReplicatorConfig()
                    .setElectionTimeout(3000))
            .buildLeader();

        Iterator<UUID> it = env.remotePeers().iterator();

        UUID node0 = it.next();
        UUID node1 = it.next();

        {
            env.emitAppendEntriesResponse(
                node0,
                term,
                true,
                0,
                0,
                0,
                0,
                0
            );

            env.emitAppendEntriesResponse(
                node1,
                term,
                true,
                0,
                0,
                0,
                0,
                0
            );

            assertFalse(env.hasProgress());

            env.replicator().appendEntries(1);
            LogEntry[] entries = entriesFromTerms(term);

            {
                assertTrue(env.hasProgress());

                Progress p = env.progress();

                assertNull(p.hardState());
                assertEquals(1, p.logEntries().size());
                assertEquals(entries[0], p.logEntries().get(0));

                assertEquals(2, p.messages().size());

                UUID first;

                {
                    OutgoingMessage msg = p.messages().get(0);
                    assertTrue(node0.equals(msg.destinationPeerId()) || node1.equals(msg.destinationPeerId()));

                    first = msg.destinationPeerId();

                    AppendEntriesRequest req = msg.message();
                    assertEquals(term, req.term());
                    assertEquals(0, req.commitIndex());
                    assertEquals(0, req.lastLogIndex());
                    assertEquals(0, req.lastLogTerm());
                    assertEquals(1, req.logEntries().length);
                    assertEquals(entries[0], req.logEntries()[0]);
                }
                {
                    OutgoingMessage msg = p.messages().get(1);
                    assertTrue(node0.equals(msg.destinationPeerId()) || node1.equals(msg.destinationPeerId()));
                    assertNotEquals(first, msg.destinationPeerId());

                    AppendEntriesRequest req = msg.message();
                    assertEquals(term, req.term());
                    assertEquals(0, req.commitIndex());
                    assertEquals(0, req.lastLogIndex());
                    assertEquals(0, req.lastLogTerm());
                    assertEquals(1, req.logEntries().length);
                    assertEquals(entries[0], req.logEntries()[0]);
                }
            }
        }

        {
            // Now one entry is enqueued for the next message.
            env.replicator().appendEntries(2);

            assertTrue(env.hasProgress());

            Progress p = env.progress();

            assertNull(p.hardState());
            assertNull(p.messages());
            assertEquals(1, p.logEntries().size());
            assertEquals(new SimulatedLogEntry(2, term, 2), p.logEntries().get(0));
        }

        {
            env.emitAppendEntriesResponse(
                node0,
                term,
                true,
                1,
                0,
                0,
                0,
                0
            );

            assertTrue(env.hasProgress());

            Progress p = env.progress();

            assertNotNull(p.hardState());
            assertEquals(1, p.hardState().commitIndex());
            assertEquals(term, p.hardState().term());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());
            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node0, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(term, req.term());
            assertEquals(1, req.commitIndex());
            assertEquals(1, req.lastLogIndex());
            assertEquals(term, req.lastLogTerm());
            assertEquals(1, req.logEntries().length);
            assertEquals(new SimulatedLogEntry(2, term, 2), req.logEntries()[0]);
        }

        {
            env.emitAppendEntriesResponse(
                node0,
                term,
                true,
                2,
                1,
                0,
                0,
                0
            );

            assertTrue(env.hasProgress());

            Progress p = env.progress();

            assertNotNull(p.hardState());
            assertEquals(2, p.hardState().commitIndex());
            assertEquals(term, p.hardState().term());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());
            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node0, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(term, req.term());
            assertEquals(2, req.commitIndex());
            assertEquals(2, req.lastLogIndex());
            assertEquals(term, req.lastLogTerm());
            assertEquals(0, req.logEntries().length);
        }

        {
            env.emitAppendEntriesResponse(
                node1,
                term,
                true,
                1,
                0,
                0,
                0,
                0
            );

            assertTrue(env.hasProgress());

            Progress p = env.progress();

            assertNull(p.hardState());
            assertNull(p.logEntries());

            assertEquals(1, p.messages().size());
            OutgoingMessage msg = p.messages().get(0);
            assertEquals(node1, msg.destinationPeerId());

            AppendEntriesRequest req = msg.message();
            assertEquals(term, req.term());
            assertEquals(2, req.commitIndex());
            assertEquals(1, req.lastLogIndex());
            assertEquals(term, req.lastLogTerm());
            assertEquals(1, req.logEntries().length);
            assertEquals(new SimulatedLogEntry(2, term, 2), req.logEntries()[0]);
        }

        {
            env.emitAppendEntriesResponse(
                node1,
                term,
                true,
                2,
                2,
                0,
                0,
                0
            );

            assertFalse(env.hasProgress());
        }
    }
}
