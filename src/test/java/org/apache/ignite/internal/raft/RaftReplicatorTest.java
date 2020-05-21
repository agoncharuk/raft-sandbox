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

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.raft.message.AppendEntriesRequest;
import org.apache.ignite.internal.raft.message.AppendEntriesResponse;
import org.apache.ignite.internal.raft.message.VoteRequest;
import org.apache.ignite.internal.raft.message.VoteResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.ignite.internal.raft.ReplicatorTestUtils.entriesFromTerms;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class RaftReplicatorTest {
    /**
     * Check that a candidate transitions to the leader state only when reached a majority, plus checks that late
     * vore responses do not alter leader state.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3, 4, 5})
    public void testElectionWinSimple(int grpSize) {
        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(grpSize)
            .buildCandidate();

        assertEquals(ReplicatorRole.CANDIDATE, env.replicator().role());

        {
            int voted = 1; // already voted for ourselves.
            int majority = grpSize / 2 + 1;
            long term = env.persistence().hardState().term();

            for (UUID voter : env.remotePeers()) {
                env.emitVoteResponse(voter, term, true);

                voted++;

                if (voted == majority) {
                    assertTrue(env.hasProgress());

                    Progress p = env.progress();

                    assertEquals(ReplicatorRole.LEADER, p.role());
                    assertEquals(ReplicatorRole.LEADER, env.replicator().role());

                    // Hard state should not change on leader transition.
                    assertNull(p.hardState());
                    assertNull(p.logEntries());

                    assertFalse(p.messages().isEmpty());

                    for (OutgoingMessage msg : p.messages()) {
                        assertTrue(env.remotePeers().contains(msg.destinationPeerId()));

                        assertTrue(msg.message() instanceof AppendEntriesRequest);

                        AppendEntriesRequest req = msg.message();
                        assertEquals(term, req.term());
                        assertEquals(env.persistence().lastLogIndex(), req.lastLogIndex());
                        assertEquals(env.persistence().entryTerm(env.persistence().lastLogIndex()), req.lastLogTerm());
                    }
                }
                else if (voted > majority) {
                    assertEquals(ReplicatorRole.LEADER, env.replicator().role());
                    assertFalse(env.replicator().hasProgress());
                }
                else {
                    assertEquals(ReplicatorRole.CANDIDATE, env.replicator().role());
                    assertFalse(env.replicator().hasProgress());
                }
            }
        }
    }

    /**
     */
    @Test
    public void testStaleVoteResponse() {
        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(3)
            .buildCandidate();

        assertEquals(ReplicatorRole.CANDIDATE, env.replicator().role());
        assertFalse(env.hasProgress());

        {
            long staleTerm = env.persistence().hardState().term() - 1;

            for (UUID peer : env.remotePeers())
                env.emitVoteResponse(peer, staleTerm, true);

            assertFalse(env.hasProgress());
            assertEquals(ReplicatorRole.CANDIDATE, env.replicator().role());
        }
    }

    /**
     * Tests that candidate may win an election after a split-vote.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3, 4, 5})
    public void testSplitVote(int grpSize) {
        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(grpSize).buildCandidate();

        {
            int majority = grpSize / 2 + 1;
            int rejected = 0;

            long term = env.persistence().hardState().term();

            for (UUID voter : env.remotePeers()) {
                env.emitVoteResponse(voter, term, rejected >= majority);

                if (rejected < majority)
                    rejected++;

                assertEquals(ReplicatorRole.CANDIDATE, env.replicator().role());
            }
        }

        assertFalse(env.hasProgress());

        // Check that the node will attempt to restart the election after a split vote.
        env.step(1000);

        {
            assertTrue(env.hasProgress());

            Progress p = env.progress();
            assertNotNull(p.hardState());

            assertEquals(3, p.hardState().term());

            assertEquals(env.localId(), p.hardState().votedFor());

            assertEquals(env.remotePeers().size(), p.messages().size());

            Set<UUID> targets = new HashSet<>();

            for (OutgoingMessage msg : p.messages()) {
                assertTrue(targets.add(msg.destinationPeerId()), "Duplicate message for peer: " +
                    msg.destinationPeerId());

                assertTrue(msg.message() instanceof VoteRequest);

                VoteRequest req = msg.message();
                assertEquals(3, req.term());
            }
        }
    }

    /**
     * Checks that a raft node votes only for a single node in the given term.
     */
    @Test
    public void testNoMultipleVotesInTerm() {
        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(5).buildFollower();

        final UUID votedFor = env.remotePeer();

        for (UUID peer : env.remotePeers()) {
            env.emitVoteRequest(peer, 1, 0, 0);

            assertTrue(env.hasProgress());

            Progress p = env.replicator().progress();

            assertEquals(ReplicatorRole.FOLLOWER, env.replicator().role());
            assertEquals(ReplicatorRole.FOLLOWER, p.role());
            assertNull(p.logEntries());

            if (votedFor.equals(peer)) {
                assertNotNull(p.hardState());
                assertEquals(1, p.hardState().term());
                assertEquals(votedFor, p.hardState().votedFor());
            }
            else
                assertNull(p.hardState());

            assertEquals(1, p.messages().size());
            OutgoingMessage msg = p.messages().get(0);

            assertEquals(peer, msg.destinationPeerId());
            assertTrue(msg.message() instanceof VoteResponse);

            VoteResponse res = msg.message();
            assertEquals(votedFor.equals(peer), res.voteGranted());
            assertEquals(1, res.term());
        }
    }

    /**
     * Checks that if a node is initialized from persistence state with vote recorder, it will not change the
     * vote.
     */
    @Test
    public void testNodeDoesNotChangeVoteAfterRestart() {
        UUID[] grp = new UUID[5];

        for (int i = 0; i < 5; i++)
            grp[i] = UUID.randomUUID();

        StandaloneEnvironment env = ReplicatorTestUtils.environment()
            .withGroup(grp)
            .withPersistence(1, grp[2])
            .buildFollower();

        // Order is important here.
        for (int i = 1; i < grp.length; i++) {
            UUID peer = grp[i];

            env.emitVoteRequest(peer, 1, 0, 0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();

            assertNull(p.hardState());
            assertNull(p.logEntries());
            assertEquals(ReplicatorRole.FOLLOWER, p.role());
            assertEquals(ReplicatorRole.FOLLOWER, env.replicator().role());

            assertEquals(1, p.messages().size());
            OutgoingMessage msg = p.messages().get(0);

            assertEquals(peer, msg.destinationPeerId());

            VoteResponse res = msg.message();

            assertEquals(grp[2].equals(peer), res.voteGranted());
            assertEquals(1, res.term());
        }
    }

    /**
     * Checks that a vote request will be rejected by a node if requester's log is not at least up-to-date with
     * the local node: the local term is greater thatn the requester term.
     */
    @Test
    public void testRejectVoteRequestStaleTerm() {
        StandaloneEnvironment env = ReplicatorTestUtils.environment()
            .withGroup(5)
            .withPersistence(2, null)
            .buildFollower();

        for (UUID peer : env.remotePeers()) {
            env.emitVoteRequest(peer, 1, 0, 0);

            assertTrue(env.hasProgress());

            Progress p = env.progress();

            assertNull(p.hardState());
            assertNull(p.logEntries());
            assertEquals(ReplicatorRole.FOLLOWER, p.role());
            assertEquals(ReplicatorRole.FOLLOWER, env.replicator().role());

            assertEquals(1, p.messages().size());

            assertEquals(peer, p.messages().get(0).destinationPeerId());

            VoteResponse res = p.messages().get(0).message();

            assertFalse(res.voteGranted());
            assertEquals(2, res.term());
        }
    }

    /**
     * Checks that a vote request will be rejected by a node if requester's log is not at least up-to-date with
     * the local node: the local last log entry term is greater than requester term.
     */
    @Test
    public void testRejectVoteRequestStaleLogByTerm() {
        long[] terms = new long[] {1, 2, 2};

        StandaloneEnvironment env = ReplicatorTestUtils.environment()
            .withGroup(2)
            .withPersistence(2, null, entriesFromTerms(terms))
            .buildFollower();

        UUID peer = env.remotePeer();

        long term = env.persistence().hardState().term() + 1;

        env.emitVoteRequest(
            peer,
            term,
            3,
            1
        );

        assertTrue(env.hasProgress());

        Progress p = env.progress();

        assertNotNull(p.hardState());
        assertEquals(term, p.hardState().term());
        // Vote should not be recorded.
        assertNull(p.hardState().votedFor());
        assertNull(p.logEntries());
        assertEquals(ReplicatorRole.FOLLOWER, p.role());
        assertEquals(ReplicatorRole.FOLLOWER, env.replicator().role());

        assertEquals(1, p.messages().size());
        assertEquals(peer, p.messages().get(0).destinationPeerId());

        VoteResponse res = p.messages().get(0).message();

        assertFalse(res.voteGranted());
        assertEquals(term, res.term());
    }

    /**
     * Checks that a vote request will be rejected by a node if requester's log is not at least up-to-date with
     * the local node: the local last log entry term is equal to the requester last log entry term, but the index
     * of the last log entry is greater than the requester last log entry.
     */
    @Test
    public void testRejectVoteRequestStaleLogByIndex() {
        long[] terms = new long[] {1, 1, 1};

        StandaloneEnvironment env = ReplicatorTestUtils.environment()
            .withGroup(2)
            .withPersistence(2, null, entriesFromTerms(terms))
            .buildFollower();

        UUID peer = env.remotePeer();

        long term = env.persistence().hardState().term() + 1;

        env.emitVoteRequest(
            peer,
            term,
            2,
            1
        );

        assertTrue(env.hasProgress());

        Progress p = env.progress();

        assertNotNull(p.hardState());
        assertEquals(term, p.hardState().term());
        // Vote should not be recorded.
        assertNull(p.hardState().votedFor());
        assertNull(p.logEntries());
        assertEquals(ReplicatorRole.FOLLOWER, p.role());
        assertEquals(ReplicatorRole.FOLLOWER, env.replicator().role());

        assertEquals(1, p.messages().size());
        assertEquals(peer, p.messages().get(0).destinationPeerId());

        VoteResponse res = p.messages().get(0).message();

        assertFalse(res.voteGranted());
        assertEquals(term, res.term());
    }

    /**
     * Tests that a follower will upgrade it's term and vote for it.
     */
    @Test
    public void testAcceptVoteWithGreaterTerm() {
        long[] terms = new long[] {1, 1, 1};

        StandaloneEnvironment env = ReplicatorTestUtils.environment()
            .withGroup(2)
            .withPersistence(2, null, entriesFromTerms(terms))
            .buildFollower();

        UUID peer = env.remotePeer();

        long term = env.persistence().hardState().term() + 1;

        env.emitVoteRequest(
            peer,
            term,
            2,
            2
        );

        assertTrue(env.hasProgress());

        Progress p = env.progress();

        assertNotNull(p.hardState());
        assertEquals(term, p.hardState().term());
        // Vote should be recorded.
        assertEquals(peer, p.hardState().votedFor());
        assertNull(p.logEntries());
        assertEquals(ReplicatorRole.FOLLOWER, p.role());
        assertEquals(ReplicatorRole.FOLLOWER, env.replicator().role());

        assertEquals(1, p.messages().size());
        assertEquals(peer, p.messages().get(0).destinationPeerId());

        VoteResponse res = p.messages().get(0).message();

        assertTrue(res.voteGranted());
        assertEquals(term, res.term());
    }

    /**
     * Tests that a node will accept vote when logs match.
     */
    @Test
    public void testAcceptVoteWithSameTermAndIndex() {
        long[] terms = new long[] {1, 1, 1};

        StandaloneEnvironment env = ReplicatorTestUtils.environment()
            .withGroup(2)
            .withPersistence(2, null, entriesFromTerms(terms))
            .buildFollower();

        UUID peer = env.remotePeer();

        long term = env.persistence().hardState().term() + 1;

        env.emitVoteRequest(
            peer,
            term,
            3,
            1
        );

        assertTrue(env.hasProgress());

        Progress p = env.progress();

        assertNotNull(p.hardState());
        assertEquals(term, p.hardState().term());
        // Vote should be recorded.
        assertEquals(peer, p.hardState().votedFor());
        assertNull(p.logEntries());
        assertEquals(ReplicatorRole.FOLLOWER, p.role());
        assertEquals(ReplicatorRole.FOLLOWER, env.replicator().role());

        assertEquals(1, p.messages().size());
        assertEquals(peer, p.messages().get(0).destinationPeerId());

        VoteResponse res = p.messages().get(0).message();

        assertTrue(res.voteGranted());
        assertEquals(term, res.term());
    }

    /**
     * Tests that a node will accept a vote with the same term and a greater index.
     */
    @Test
    public void testAcceptVoteWithSameTermAndGreaterIndex() {
        long[] terms = new long[] {1, 1, 1};

        StandaloneEnvironment env = ReplicatorTestUtils.environment()
            .withGroup(2)
            .withPersistence(2, null, entriesFromTerms(terms))
            .buildFollower();

        UUID peer = env.remotePeer();

        long term = env.persistence().hardState().term() + 1;

        env.emitVoteRequest(
            peer,
            term,
            4,
            1
        );

        assertTrue(env.hasProgress());

        Progress p = env.progress();

        assertNotNull(p.hardState());
        assertEquals(term, p.hardState().term());
        // Vote should be recorded.
        assertEquals(peer, p.hardState().votedFor());
        assertNull(p.logEntries());
        assertEquals(ReplicatorRole.FOLLOWER, p.role());
        assertEquals(ReplicatorRole.FOLLOWER, env.replicator().role());

        assertEquals(1, p.messages().size());
        assertEquals(peer, p.messages().get(0).destinationPeerId());

        VoteResponse res = p.messages().get(0).message();

        assertTrue(res.voteGranted());
        assertEquals(term, res.term());
    }

    /**
     * Tests that a caniddate will convert to follower if a vote request with a greater term is received.
     */
    @Test
    public void testCandidateConvertsToFollowerOnGreaterTermVoteRequest() {
        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(3)
            .buildCandidate();

        long term = env.persistence().hardState().term() + 1;

        UUID peer = env.remotePeer();

        env.emitVoteRequest(
            peer,
            term,
            0,
            0);

        assertTrue(env.hasProgress());

        Progress p = env.progress();

        assertNotNull(p.hardState());
        assertEquals(term, p.hardState().term());
        // Vote should be recorded.
        assertEquals(peer, p.hardState().votedFor());
        assertNull(p.logEntries());
        assertEquals(ReplicatorRole.FOLLOWER, p.role());
        assertEquals(ReplicatorRole.FOLLOWER, env.replicator().role());

        assertEquals(1, p.messages().size());
        assertEquals(peer, p.messages().get(0).destinationPeerId());

        VoteResponse res = p.messages().get(0).message();

        assertTrue(res.voteGranted());
        assertEquals(term, res.term());
    }

    /**
     * Tests that a candidate will convert to follower on a vote response with a greater term.
     */
    @Test
    public void testCandidateConvertsToFollowerOnGreaterTermVoteResponse() {
        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(3)
            .buildCandidate();

        long term = env.persistence().hardState().term() + 1;

        env.emitVoteResponse(
            env.remotePeers().iterator().next(),
            term,
            false);

        assertTrue(env.hasProgress());

        Progress p = env.progress();

        assertNotNull(p.hardState());
        assertEquals(term, p.hardState().term());
        assertNull(p.hardState().votedFor());
        assertEquals(ReplicatorRole.FOLLOWER, p.role());
        assertEquals(ReplicatorRole.FOLLOWER, env.replicator().role());
        assertNull(p.logEntries());
        assertNull(p.messages());
    }

    /**
     * Tests that a candidate will convert to follower on an append entries request with a greater term.
     */
    @Test
    public void testCandidateConvertsToFollowerOnGreaterTermAppendEntriesRequest() {
        StandaloneEnvironment env = ReplicatorTestUtils.environment().withGroup(3)
            .buildCandidate();

        long term = env.persistence().hardState().term() + 1;
        UUID peer = env.remotePeer();

        env.emitAppendEntriesRequest(
            peer,
            term,
            0,
            0,
            RaftReplicator.EMPTY,
            0
        );

        assertTrue(env.hasProgress());

        Progress p = env.progress();

        assertNotNull(p.hardState());
        assertEquals(term, p.hardState().term());
        assertNull(p.hardState().votedFor());
        assertNull(p.logEntries());
        assertEquals(ReplicatorRole.FOLLOWER, p.role());
        assertEquals(ReplicatorRole.FOLLOWER, env.replicator().role());

        assertEquals(1, p.messages().size());
        assertEquals(peer, p.messages().get(0).destinationPeerId());

        AppendEntriesResponse res = p.messages().get(0).message();

        assertEquals(term, res.term());
        assertTrue(res.success());
        assertEquals(0, res.matchIndex());
        assertEquals(0, res.conflictStart());
        assertEquals(0, res.conflictIndex());
        assertEquals(0, res.conflictTerm());

        assertEquals(peer, env.replicator().leaderId());
    }
}
