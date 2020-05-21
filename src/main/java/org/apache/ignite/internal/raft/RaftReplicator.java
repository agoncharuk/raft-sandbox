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
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.raft.message.AppendEntriesRequest;
import org.apache.ignite.internal.raft.message.AppendEntriesResponse;
import org.apache.ignite.internal.raft.message.LogEntry;
import org.apache.ignite.internal.raft.message.MessageFactory;
import org.apache.ignite.internal.raft.message.RaftMessage;
import org.apache.ignite.internal.raft.message.VoteRequest;
import org.apache.ignite.internal.raft.message.VoteResponse;
import org.apache.ignite.internal.raft.service.ConfigurationState;
import org.apache.ignite.internal.raft.service.HardState;
import org.apache.ignite.internal.raft.service.TermRange;
import org.apache.ignite.internal.raft.service.PersistenceService;
import org.apache.ignite.internal.raft.service.TimeService;

/**
 * Single-threaded raft log replicator.
 *
 * Outstanding items:
 * RAFT-0 Read actions
 * RAFT-1 Pre-vote
 * RAFT-2 Configuration change via joint consensus
 * RAFT-3 Metrics (dropped, skipped, sent, received messages, etc)
 * RAFT-4 Memory limits and backpressure control
 */
public class RaftReplicator {
    /** */
    public static final LogEntry[] EMPTY = new LogEntry[0];

    /** */
    private final ReplicatorConfig cfg;

    /** */
    private final TimeService timeSvc;

    /** */
    private final MessageFactory msgFactory;

    /** */
    private final RaftLog raftLog;

    /** */
    private ReplicatorState state;

    /** */
    private final HardStateHolder hardStateHolder;

    /** */
    private List<OutgoingMessage> msgs = new ArrayList<>();

    /** */
    @SuppressWarnings("FieldMayBeFinal")
    private ConfigurationState confState;

    /**
     */
    public RaftReplicator(
        ReplicatorConfig cfg,
        PersistenceService persistenceSvc,
        TimeService timeSvc,
        MessageFactory msgFactory
    ) {
        validateConfig(cfg);

        confState = persistenceSvc.confState();

        this.cfg = cfg;
        this.timeSvc = timeSvc;
        this.msgFactory = msgFactory;

        HardState hardState = persistenceSvc.hardState();

        hardStateHolder = new HardStateHolder(hardState);
        raftLog = new RaftLog(persistenceSvc, msgFactory);

        updateState(new Follower(
            hardState.term(),
            // Intentionally initiate the first election faster
            timeSvc.nextRandomizedDeadline(cfg.getElectionTimeout(), 0.5f)));
    }

    /**
     * @return Current replicator role.
     */
    public ReplicatorRole role() {
        return state.role();
    }

    /**
     * @return ID of the current leader, if exists and known.
     */
    public UUID leaderId() {
        return state.leaderId();
    }

    /**
     * Requests replicator to append entries to the replicated log. The request will modify the progress
     * which should be processed by the replicator user.
     * The append request may not accept all entries because of the maximum number of entries pending to be persisted
     * or the maximum number of non-committed entries. The number of entries accepted for replication will be
     * reflected in the returned receipt.
     *
     * @param entries Entries to append to the replicated log.
     * @return Append receipt that reflects the number of entries accepted for replication and leader term for
     *      the requested to validate the entry commit.
     */
    public <T> AppendReceipt appendEntries(T... entries) {
        switch (state().role()) {
            case LEADER:
                long[] idxs = raftLog.addUnstable(term(), entries);

                maybeBroadcastAppendEntries();

                return new AppendReceipt(term(), idxs);

            case FOLLOWER:
                Follower follower = state();

                throw new NotLeaderException("Failed to append log entries (current node is not a quorum leader) " +
                    "[leader=" + follower.leaderId() + ']', follower.leaderId());

            case CANDIDATE:
            default:
                throw new NotLeaderException("Failed to append log entries (local node is campaigning for leadership)",
                    null);
        }
    }

    /**
     * Tick function should be called periodically to run the replicator background tasks, such as election timeout
     * handling and heartbeat emission. As a result of tick progress may be updated. The replicator user should
     * process the progress in order for replicator to move forward.
     */
    public void onTick() {
        long now = timeSvc.currentTimeMillis();

        switch (state.role()) {
            case LEADER:
                // Replicate
                maybeBroadcastAppendEntries();

                break;

            case FOLLOWER:
                Follower follower = (Follower)state;

                if (follower.electionDeadline() <= now)
                    startElection();

                break;

            case CANDIDATE:
                Candidate candidate = (Candidate)state;

                if (now >= candidate.electionDeadline())
                    startElection();
                // TODO while we are waiting election timeout, try to re-send vote requests to those
                // TODO who did not respond

                break;
        }
    }

    /**
     * Step function is called each time a Raft replicator receives a message from a remote peer. As a result
     * of message processing progress may be updated. The replicator user should process the progress in
     * order for replicator to move forward.
     *
     * @param peerId Peer ID that sent the message.
     * @param msg Message to process.
     */
    public void step(UUID peerId, RaftMessage msg) {
        if (!confState.hasPeer(peerId)) {
            // Received a message from node outside of the cluster configuration, ignore.
            return;
        }

        if (term() < msg.term())
            convertToFollower(msg.term());

        switch (msg.type()) {
            case VOTE_REQUEST:
                processVoteRequest(peerId, (VoteRequest)msg);

                break;

            case VOTE_RESPONSE:
                processVoteResponse(peerId, (VoteResponse)msg);

                break;

            case APPEND_ENTRIES_REQUEST:
                processAppendEntriesRequest(peerId, (AppendEntriesRequest)msg);

                break;

            case APPEND_ENTRIES_RESPONSE:
                processAppendEntriesResponse(peerId, (AppendEntriesResponse)msg);

                break;
        }
    }

    /**
     * @return {@code true} if there is an outstanding progress that needs to be handled.
     */
    public boolean hasProgress() {
        return hardStateHolder.hasProgress() || raftLog.hasProgress() || !msgs.isEmpty();
    }

    /**
     * Captures outstanding progress that must be processed by the replicator user.
     * The persistent state of the progress (hard state and log entries) must be fully persisted before calling
     * any other public methods of the replicator.
     *
     * @return Progress object or {@code null} if there are no changes in the replicator state.
     */
    public Progress progress() {
        if (!hasProgress())
            return null;

        // TODO handle configuration changes: we need to add ConfState changes to the progress.
        Progress progress = new Progress(role(), state.leaderId());

        hardStateHolder.fillProgress(progress);
        raftLog.fillProgress(progress);

        if (!msgs.isEmpty()) {
            progress.setMessages(msgs);

            msgs = new ArrayList<>();
        }

        return progress;
    }

    /**
     * Validates configuration to have proper values.
     */
    private void validateConfig(ReplicatorConfig cfg) {
        if (cfg.getElectionTimeout() <= 0)
            throw new UnrecoverableException("Configuration validation failed: electionTimeout " +
                "must be greater than 0: " + cfg.getElectionTimeout());
    }

    /**
     */
    private void processVoteRequest(UUID peerId, VoteRequest msg) {
        switch (state.role()) {
            case FOLLOWER: {
                boolean voteGranted = false;

                if (term() == msg.term()) {
                    if (votedFor() == null) {
                        // We can grant the vote only if the local log is up-to-date with the remote candidate.
                        if (logIsUpToDate(msg.lastLogTerm(), msg.lastLogIndex())) {
                            updateCurrentTermAndRecordVote(msg.term(), msg.candidateId());

                            voteGranted = true;
                        }
                    }
                    else
                        voteGranted = msg.candidateId().equals(votedFor());
                }

                sendMessage(
                    msgFactory.createVoteResponse(
                        term(),
                        voteGranted
                    ),
                    peerId
                );

                break;
            }

            case LEADER: {
                // If this node is a leader during this particular term, reject the vote.
                sendMessage(
                    msgFactory.createVoteResponse(
                        term(),
                        false
                    ),
                    peerId
                );

                break;
            }

            case CANDIDATE: {
                // Reject the vote since we already voted for ourself.
                sendMessage(
                    msgFactory.createVoteResponse(
                        msg.term(),
                        false
                    ),
                    peerId
                );

                break;
            }
        }
    }

    /**
     */
    private void processVoteResponse(UUID peerId, VoteResponse msg) {
        if (state.role() == ReplicatorRole.CANDIDATE) {
            Candidate candidate = (Candidate)state;

            if (msg.voteGranted()) {
                candidate.onVoteAccepted(msg.term(), peerId);

                if (candidate.wonElection())
                    convertToLeader();
            }
        }
    }

    /**
     */
    private void processAppendEntriesRequest(UUID peerId, AppendEntriesRequest msg) {
        // Ignore stale messages.
        if (msg.term() < term()) {
            sendMessage(
                msgFactory.createAppendEntriesResponse(
                    term(),
                    false,
                    0,
                    0,
                    0,
                    0,
                    0
                ),
                peerId
            );

            return;
        }

        if (canAppendEntries(msg.lastLogIndex(), msg.lastLogTerm())) {
            // The persistence service may omit several last entries if the load is too high.
            // We will reply with last written index to leader as match index so that leader will
            // re-attempt to replicate these entries.
            long lastWritten = raftLog.addUnstable(msg.logEntries());
            long oldCommitIdx = commitIndex();
            long newCommitIdx = Math.max(oldCommitIdx, Math.min(msg.commitIndex(), lastWritten));

            // It is ok to send a response before we actually advance the commit index. Even if
            // the follower crashes, it will receive updated commit index with the next heartbeat.
            sendMessage(
                msgFactory.createAppendEntriesResponse(
                    term(),
                    true,
                    lastWritten,
                    newCommitIdx,
                    0,
                    0,
                    0
                ),
                peerId
            );

            if (oldCommitIdx != newCommitIdx)
                updateCommitIndex(newCommitIdx);
        }
        else {
            // The logs did not match.
            // According to the paper, we should truncate the log here.
            // For now, we do not request the replicator user to truncate the log. Instead, we request the user to
            // identify the case when an entry with a smaller index is logged and truncate all following entries then.
            // However, we do update the volatile lastLogIndex in raftLog to correctly determine the matchIndex later.
            TermRange bound = raftLog.fetchTermStart(msg.lastLogIndex() - 1);

            raftLog.truncate(msg.lastLogIndex());

            sendMessage(
                msgFactory.createAppendEntriesResponse(
                    term(),
                    false,
                    0,
                    0,
                    bound.startIndex(),
                    bound.endIndex(),
                    bound.term()
                ),
                peerId
            );
        }

        Follower follower = state();

        follower.electionDeadline(timeSvc.nextRandomizedDeadline(cfg.getElectionTimeout(), 2f));
        follower.leaderId(peerId);
    }

    /**
     */
    private void processAppendEntriesResponse(UUID peerId, AppendEntriesResponse res) {
        if (res.term() != term() || role() != ReplicatorRole.LEADER) {
            // TODO message dropped.
            return;
        }

        Leader leader = state();

        RemoteFollowerState follower = leader.remoteFollower(peerId);

        if (follower != null) {
            follower.updateNextEntries(
                res.success(),
                res.matchIndex(),
                res.commitIndex(),
                divergenceBound(res));

            if (res.success()) {
                long oldCommitIdx = commitIndex();

                long newCommitIdx = leader.effectiveCommitIndex();

                if (newCommitIdx > oldCommitIdx && term() == raftLog.entryTerm(newCommitIdx))
                    updateCommitIndex(newCommitIdx);
            }

            maybeSendAppendEntries(peerId, follower);
        }
    }

    /**
     * Moves the local replicator to the leader state. Must be called only if local node was campaigning and won the
     * election.
     * @throws UnrecoverableException if local replicator is not a candidate.
     */
    private void convertToLeader() {
        if (role() != ReplicatorRole.CANDIDATE)
            throw new UnrecoverableException("Invalid state to transition to leader: " + state());

        updateState(new Leader(term(), confState, raftLog.lastLogIndex()));

        maybeBroadcastAppendEntries();
    }

    /**
     * Unconditionally moves the local replicator to the follower state with the given term.
     *
     * @param term Follower term.
     */
    private void convertToFollower(long term) {
        updateCurrentTermAndRecordVote(term, null);

        updateState(new Follower(
            term,
            timeSvc.nextRandomizedDeadline(cfg.getElectionTimeout(), 2f)));
    }

    /**
     * Unconditionally moves the local replicator to the candidate state incrementing the local term.
     */
    private void startElection() {
        long newTerm = term() + 1;

        updateCurrentTermAndRecordVote(newTerm, confState.localId());

        Candidate candidate = updateState(new Candidate(
            newTerm,
            confState,
            timeSvc.nextRandomizedDeadline(cfg.getElectionTimeout(), 2f)));

        // Vote for ourself immidiately.
        candidate.onVoteAccepted(newTerm, confState.localId());

        for (UUID peerId : confState.group()) {
            if (peerId != confState.localId()) {
                sendMessage(
                    msgFactory.createVoteRequest(
                        newTerm,
                        confState.localId(),
                        raftLog.lastLogIndex(),
                        raftLog.entryTerm(raftLog.lastLogIndex())
                    ),
                    peerId
                );
            }
        }
    }

    /**
     * Will send append entries request to the remote follower if:
     * <ul>
     *     <li>We do not know the match index for the remote follower and leader is not waiting a response from
     *     the follower</li>
     *     <li>There are pending log entries that are not yet replicated and leader is not waiting a response
     *     from the follower</li>
     *     <li>There are pending log entries that are not yet replicated and follower timed out</li>
     *     <li>There are no entries to replicate, but it's time to send a heartbeat</li>
     * </ul>
     *
     * @param peerId Peer ID to send entries to.
     * @param follower Remote follower state.
     */
    private void maybeSendAppendEntries(UUID peerId, RemoteFollowerState follower) {
        long now = timeSvc.currentTimeMillis();

        LogEntry[] entries = null;
        long nextIdx = follower.nextIndex();

        if (!follower.matched()) {
            if (!follower.waitingResponse(now))
                entries = EMPTY;
        }
        else {
            if (nextIdx <= raftLog.lastLogIndex()) { // we have an entry to append
                if (!follower.waitingResponse(now))
                    entries = raftLog.fetchEntries(nextIdx, cfg.getAppendEntriesBatchSize());
            }
            // Heartbeat
            else {
                // Send heartbeat on timeout or eagerly send the heartbeat to advance remote follower commit index.
                if (follower.hearbeatDue(now) || follower.knownCommitIndex() != commitIndex())
                    entries = EMPTY;
            }
        }

        if (entries != null) {
            long logIdx = nextIdx > 0 ? nextIdx - 1 : 0;
            long logTerm = raftLog.entryTerm(logIdx);

            // TODO figure out how timeouts are calculated.
            long resDeadline = now + cfg.getElectionTimeout() / 5;
            long heartbeatDeadline = now + cfg.getElectionTimeout() / 5;

            follower.startRequest(resDeadline, heartbeatDeadline);

            sendMessage(
                msgFactory.createAppendEntriesRequest(
                    term(),
                    logIdx,
                    logTerm,
                    entries,
                    commitIndex()
                ),
                peerId
            );
        }
    }

    /**
     * Checks if the local log is up-to-date with campaigning candidate so we can vote for this candidate.
     *
     * @param msgLastLogTerm Remote candidate last log term.
     * @param msgLastLogIdx Remote candidate last log index.
     * @return {@code true} if local log is up-to-date with the remote candidate, {@code false} otherwise.
     */
    private boolean logIsUpToDate(long msgLastLogTerm, long msgLastLogIdx) {
        long lastLogIdx = raftLog.lastLogIndex();

        if (lastLogIdx == 0)
            // An empty log is always up-to-date with any node.
            return true;

        // According to §5.4
        // If the logs have last entries with different terms, then
        //the log with the later term is more up-to-date. If the logs
        //end with the same term, then whichever log is longer is
        //more up-to-date.
        long lastLogTerm = raftLog.entryTerm(lastLogIdx);

        return lastLogTerm < msgLastLogTerm ||
            (lastLogTerm == msgLastLogTerm && lastLogIdx <= msgLastLogIdx);
    }

    /**
     * Checks if remote leader can append entries starting from the {@code msgLogIdx + 1}.
     *
     * @param msgLogIdx Probe log index from remote leader.
     * @param msgLogTerm Probe log term at the {@code msgLogIdx} position from remote leader.
     * @return {@code true} if local log matches remote leader at the given position, {@code false} otherwise.
     */
    private boolean canAppendEntries(long msgLogIdx, long msgLogTerm) {
        long lastLogIdx = raftLog.lastLogIndex();

        // If local log is empty, accept only if leader starts from the beginning.
        if (lastLogIdx == 0)
            return msgLogIdx == 0;

        // Do not accept if we are behind the leader.
        if (lastLogIdx < msgLogIdx)
            return false;

        // Here lastLogIdx >= msgLogIdx, so we know that the entry msgLogIdx exists.
        long entryTerm = raftLog.entryTerm(msgLogIdx);

        // Accept only if log entry term in local log matches remote entry term at the same position.
        return entryTerm == msgLogTerm;
    }

    /**
     * This method determines how much backwards a leader may shift the divergence bound based on the follower reply.
     * This method will return a meaningful result only if {@code res.success()} is {@code false}.
     */
    private long divergenceBound(AppendEntriesResponse res) {
        if (res.success())
            return 0;

        if (res.conflictIndex() == 0)
            return 0;

        TermRange range = raftLog.fetchTermStart(res.conflictIndex());

        // If terms match, return the index following the conflict end on the remote node:
        // Index      1  2  3  4  5  6  7  8
        // Follower  [3  3  3  3  3]
        // Leader    [3  3  3  3  3  3  3  3]
        // Result                    *
        if (range.term() == res.conflictTerm())
            return res.conflictIndex() + 1;

        // If terms do not match, choose the highest start bound between two ranges.
        // Index      1  2  3  4  5  6  7  8
        // Follower   4  4  4 [6  6]
        // Leader     7  7 [9  9  9  9  9  9]
        // Result              *
        //
        // Index      1  2  3  4  5  6  7  8
        // Follower   4 [6  6  6  6]
        // Leader     7  7 [9  9  9  9  9  9]
        // Result              *
        //
        // Index      1  2  3  4  5  6  7  8
        // Follower   4 [6  6]
        // Leader     7  7 [9]
        // Result           *
        //
        // Index      1  2  3  4  5  6  7  8
        // Follower   4  4 [6  6  6]
        // Leader     7  7 [9  9  9  9  9  9]
        // Result              *
        //
        // Index      1  2  3  4  5  6  7  8
        // Follower   4  4 [6]
        // Leader     7  7 [9  9  9  9  9  9]
        // Result           *
        while (range.startIndex() > 1 && range.startIndex() >= res.conflictStart()) {
            TermRange prev = raftLog.fetchTermStart(range.startIndex() - 1);

            if (prev.term() != res.conflictTerm())
                range = prev;
            else
                break;
        }

        return Math.max(res.conflictStart() + 1, range.startIndex() + 1);
    }

    /**
     * Will send append entries requests to all peers in the replication group.
     * This method must be called only when local replicator is in the leader state.
     * 
     * @see #maybeSendAppendEntries(UUID, RemoteFollowerState)
     */
    private void maybeBroadcastAppendEntries() {
        Leader leader = state();

        // TODO handle configuration change here.
        for (UUID peerId : confState.group()) {
            if (peerId != confState.localId()) {
                RemoteFollowerState follower = leader.remoteFollower(peerId);

                if (follower != null)
                    maybeSendAppendEntries(peerId, follower);
            }
        }
    }

    /**
     * Enqueues a message to be sent to the given destination peer. The queued messages will be returned to the
     * replicator user via the progress. The replicator user must persist the hard state and log entries prior
     * to sending any outgoing messages.
     *
     * @param msg Message to send.
     * @param dstPeerId Destination peer ID.
     */
    private void sendMessage(RaftMessage msg, UUID dstPeerId) {
        if (msgs == null)
            msgs = new ArrayList<>(3 * confState.group().length);

        msgs.add(new OutgoingMessage(dstPeerId, msg));
    }

    /**
     * @return Current hard state (pending, if exists, or the persisted hard state captured by {@code progress()} call).
     */
    private HardState hardState() {
        return hardStateHolder.pending == null ? hardStateHolder.stable : hardStateHolder.pending;
    }

    /**
     * Records new term and new vote for the given term. Will create pending hard state if it does not yet exist.
     * {@code newTerm} must be greater than already recorded term and {@code vote} must be set only if the node
     * did not record the vote for the given term.
     *
     * @param newTerm New term.
     * @param vote New vote in the given term.
     */
    private void updateCurrentTermAndRecordVote(long newTerm, UUID vote) {
        HardState state = hardState();

        if (state.term() > newTerm)
            throw new UnrecoverableException("Attempted to update term to a smaller value [old=" + state.term() +
                ", newTerm=" + newTerm + ']');

        if (state.votedFor() != null && !state.votedFor().equals(vote) && newTerm == state.term())
            throw new UnrecoverableException("Attempted to change the vote [term=" + state.term() +
                ", oldVote=" + state.votedFor() + ", newVote=" + vote + ']');

        if (state.term() != newTerm || !Objects.equals(state.votedFor(), vote))
            hardStateHolder.pending = new HardState(newTerm, vote, state.commitIndex());
    }

    /**
     * Records a new commit index. Will create pending hard state if it does not yet exist. No-op if new
     * commit index is smaller or equal than already recorder commit index.
     *
     * @param commitIdx New commit index.
     */
    private void updateCommitIndex(long commitIdx) {
        HardState state = hardState();

        if (commitIdx > state.commitIndex())
            hardStateHolder.pending = new HardState(state.term(), state.votedFor(), commitIdx);
    }

    /**
     * @return Term recorded in the latest hard state.
     */
    private long term() {
        return hardState().term();
    }

    /**
     * @return Commit index recotded in the latest hard state.
     */
    private long commitIndex() {
        return hardState().commitIndex();
    }

    /**
     * @return Vote for the term recorded in the latest hard state.
     */
    private UUID votedFor() {
        return hardState().votedFor();
    }

    /**
     * Updates current replicator state to the given state.
     *
     * @param state State object.
     * @return The argument.
     */
    private <S extends ReplicatorState> S updateState(S state) {
        this.state = state;

        return state;
    }

    /**
     * Utility method that unsafely casts the current state to the called type.
     *
     * @return Currest state cast as {@code T}.
     */
    private <S extends ReplicatorState> S state() {
        return (S)state;
    }

    /**
     */
    private static class HardStateHolder {
        /** */
        private HardState stable;

        /** */
        private HardState pending;

        /** */
        private HardStateHolder(HardState stable) {
            this.stable = stable;
        }

        /**
         * @return {@code true} if there are changes in the hard state that need to be persisted to disk.
         */
        private boolean hasProgress() {
            return pending != null;
        }

        /**
         * Captures the progress to be persisted hard state.
         * @param progress Progress object to fill.
         */
        private void fillProgress(Progress progress) {
            progress.setHardState(pending);

            if (pending != null) {
                stable = pending;

                pending = null;
            }
        }
    }
}
