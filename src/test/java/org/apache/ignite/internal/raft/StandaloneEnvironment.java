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
import org.apache.ignite.internal.raft.message.LogEntry;
import org.apache.ignite.internal.raft.message.MessageFactory;
import org.apache.ignite.internal.raft.message.SimulatedMessageFactory;
import org.apache.ignite.internal.raft.service.ConfigurationState;
import org.apache.ignite.internal.raft.service.SimulatedPersistenceService;
import org.apache.ignite.internal.raft.service.SimulatedTimeSource;
import org.apache.ignite.internal.raft.service.TimeServiceImpl;

/**
 *
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class StandaloneEnvironment {
    /** */
    private final ConfigurationState grp;

    /** */
    private final Set<UUID> rmtPeers;

    /** */
    private final RaftReplicator replicator;

    /** */
    private final SimulatedPersistenceService persistence;

    /** */
    private final SimulatedTimeSource time;

    /** */
    private final MessageFactory msg;

    /**
     */
    StandaloneEnvironment(
        ReplicatorConfig cfg,
        SimulatedPersistenceService persistence,
        SimulatedTimeSource time
    ) {
        this.persistence = persistence;
        this.time = time;

        grp = persistence.confState();

        rmtPeers = new HashSet<>();

        for (UUID id : grp.group()) {
            if (!id.equals(grp.localId()))
                rmtPeers.add(id);
        }

        msg = new SimulatedMessageFactory();

        replicator = new RaftReplicator(
            cfg,
            persistence,
            new TimeServiceImpl(time, null),
            msg
        );
    }

    /**
     * @return Any remote peer ID.
     */
    public UUID remotePeer() {
        return rmtPeers.iterator().next();
    }

    /**
     * @return Set of remote peer IDs.
     */
    public Set<UUID> remotePeers() {
        return rmtPeers;
    }

    /**
     */
    public UUID localId() {
        return grp.localId();
    }

    /**
     */
    public RaftReplicator replicator() {
        return replicator;
    }

    /**
     */
    public SimulatedPersistenceService persistence() {
        return persistence;
    }

    /**
     * Advances replicator for {@code ms} milliseconds in steps of {@code tick} milliseconds.
     */
    public void step(long ms) {
        long tick = 50;
        long start = time.currentTimeMillis();
        long remainder = ms;

        while (time.currentTimeMillis() < start + ms) {
            long nextTick = (time.currentTimeMillis() / tick + 1) * tick;

            long delta = Math.min(remainder, nextTick - time.currentTimeMillis());

            time.step(delta);

            remainder -= delta;

            if (time.currentTimeMillis() % tick == 0)
                replicator.onTick();
        }
    }

    /**
     */
    public void emitVoteResponse(UUID from, long term, boolean accepted) {
        replicator.step(from,
            msg.createVoteResponse(
                term,
                accepted));
    }

    /**
     */
    public void emitVoteRequest(UUID from, long term, long lastLogIdx, long lastLogTerm) {
        replicator.step(from,
            msg.createVoteRequest(
                term,
                from,
                lastLogIdx,
                lastLogTerm));
    }

    /**
     */
    public void emitAppendEntriesRequest(
        UUID from,
        long term,
        long lastLogIdx,
        long lastLogTerm,
        LogEntry[] entries,
        long commitIdx
    ) {
        replicator.step(from,
            msg.createAppendEntriesRequest(
                term,
                lastLogIdx,
                lastLogTerm,
                entries,
                commitIdx));
    }

    /**
     */
    public void emitAppendEntriesResponse(
        UUID from,
        long term,
        boolean success,
        long matchIdx,
        long commitIdx,
        long conflictStart,
        long conflictIdx, long conflictTerm
    ) {
        replicator.step(from,
            msg.createAppendEntriesResponse(
                term,
                success,
                matchIdx,
                commitIdx,
                conflictStart,
                conflictIdx,
                conflictTerm));
    }

    /**
     */
    public boolean hasProgress() {
        return replicator.hasProgress();
    }

    /**
     * Will capture progress from the replicator, if any, and persist hard state and log entries to
     * persistence service.
     */
    public Progress progress() {
        Progress p = replicator.progress();

        if (p != null) {
            if (p.hardState() != null)
                persistence.hardState(p.hardState());

            if (p.logEntries() != null)
                persistence.appendEntries(p.logEntries());
        }

        return p;
    }
}
