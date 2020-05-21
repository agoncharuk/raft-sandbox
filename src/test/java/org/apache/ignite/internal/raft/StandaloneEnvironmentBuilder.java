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
import org.apache.ignite.internal.raft.message.AppendEntriesRequest;
import org.apache.ignite.internal.raft.message.LogEntry;
import org.apache.ignite.internal.raft.message.VoteRequest;
import org.apache.ignite.internal.raft.service.ConfigurationState;
import org.apache.ignite.internal.raft.service.HardState;
import org.apache.ignite.internal.raft.service.PersistenceService;
import org.apache.ignite.internal.raft.service.SimulatedPersistenceService;
import org.apache.ignite.internal.raft.service.SimulatedTimeSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class StandaloneEnvironmentBuilder {
    /** */
    private UUID[] grp;

    /** */
    private long term = 1;

    /** */
    private UUID votedFor;

    /** */
    private LogEntry[] entries = RaftReplicator.EMPTY;

    /** */
    private SimulatedTimeSource time = new SimulatedTimeSource(0);

    /** */
    private ReplicatorConfig cfg = new ReplicatorConfig().setElectionTimeout(1000);

    /**
     */
    public StandaloneEnvironmentBuilder withGroup(int grpSize) {
        grp = new UUID[grpSize];

        for (int i = 0; i < grpSize; i++)
            grp[i] = UUID.randomUUID();

        return this;
    }

    /**
     */
    public StandaloneEnvironmentBuilder withGroup(UUID[] grp) {
        this.grp = grp;

        return this;
    }

    /**
     */
    public StandaloneEnvironmentBuilder withPersistence(long term, UUID votedFor) {
        return withPersistence(term, votedFor, entries);
    }

    /**
     */
    public StandaloneEnvironmentBuilder withPersistence(long term, UUID votedFor, LogEntry[] entries) {
        this.term = term;
        this.votedFor = votedFor;
        this.entries = entries;

        return this;
    }

    /**
     */
    public StandaloneEnvironmentBuilder withElectionTimeout(long timeout) {
        cfg.setElectionTimeout(timeout);

        return this;
    }

    /**
     */
    public StandaloneEnvironmentBuilder withReplicatorConfig(ReplicatorConfig cfg) {
        this.cfg = cfg;

        return this;
    }

    /**
     */
    public StandaloneEnvironmentBuilder withStartTime(long startTime) {
        time = new SimulatedTimeSource(startTime);

        return this;
    }

    /**
     */
    public StandaloneEnvironmentBuilder withTimeService(SimulatedTimeSource time) {
        this.time = time;

        return this;
    }

    /**
     */
    public StandaloneEnvironment buildFollower() {
        StandaloneEnvironment env = new StandaloneEnvironment(
            cfg,
            new SimulatedPersistenceService(
                new HardState(term, votedFor, 0),
                new ConfigurationState(grp[0], grp),
                entries),
            time
        );

        assertEquals(ReplicatorRole.FOLLOWER, env.replicator().role());
        assertFalse(env.replicator().hasProgress());

        return env;
    }

    /**
     */
    public StandaloneEnvironment buildCandidate() {
        StandaloneEnvironment env = buildFollower();

        SimulatedPersistenceService pSvc = env.persistence();
        long oldTerm = pSvc.hardState().term();

        env.step(cfg.getElectionTimeout());

        assertTrue(env.replicator().hasProgress());

        assertEquals(ReplicatorRole.CANDIDATE, env.replicator().role());

        Progress p = env.progress();

        assertNotNull(p.hardState());
        assertFalse(p.messages().isEmpty());

        assertEquals(oldTerm + 1, p.hardState().term());
        assertEquals(pSvc.confState().localId(), p.hardState().votedFor());
        assertNull(p.logEntries());
        assertEquals(env.remotePeers().size(), p.messages().size());

        for (OutgoingMessage msg : p.messages()) {
            assertTrue(env.remotePeers().contains(msg.destinationPeerId()));

            assertTrue(msg.message() instanceof VoteRequest);

            VoteRequest req = msg.message();

            assertEquals(oldTerm + 1, req.term());
            assertEquals(pSvc.lastLogIndex(), req.lastLogIndex());
            assertEquals(pSvc.entryTerm(pSvc.lastLogIndex()), req.lastLogTerm());
        }

        return env;
    }

    /**
     */
    public StandaloneEnvironment buildLeader() {
        StandaloneEnvironment env = buildCandidate();

        PersistenceService pSvc = env.persistence();

        long term = pSvc.hardState().term();

        for (UUID peer : env.remotePeers())
            env.emitVoteResponse(peer, term, true);

        assertEquals(ReplicatorRole.LEADER, env.replicator().role());

        assertTrue(env.hasProgress());

        Progress p = env.progress();
        assertEquals(ReplicatorRole.LEADER, p.role());
        assertNull(p.hardState());
        assertNull(p.logEntries());

        assertEquals(env.remotePeers().size(), p.messages().size());

        // The new leader prodices append entries request immediately. Consume the messages if requested.
        for (OutgoingMessage msg : p.messages()) {
            assertTrue(env.remotePeers().contains(msg.destinationPeerId()));

            assertTrue(msg.message() instanceof AppendEntriesRequest);

            AppendEntriesRequest req = msg.message();

            assertEquals(term, req.term());
            assertEquals(pSvc.lastLogIndex(), req.lastLogIndex());
            assertEquals(pSvc.entryTerm(pSvc.lastLogIndex()), req.lastLogTerm());
        }

        return env;
    }
}
