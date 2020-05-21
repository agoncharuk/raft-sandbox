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

package org.apache.ignite.internal.raft.simulation;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.raft.AppendReceipt;
import org.apache.ignite.internal.raft.NotLeaderException;
import org.apache.ignite.internal.raft.OutgoingMessage;
import org.apache.ignite.internal.raft.Progress;
import org.apache.ignite.internal.raft.RaftReplicator;
import org.apache.ignite.internal.raft.ReplicatorConfig;
import org.apache.ignite.internal.raft.ReplicatorRole;
import org.apache.ignite.internal.raft.message.LogEntry;
import org.apache.ignite.internal.raft.message.RaftMessage;
import org.apache.ignite.internal.raft.message.SimulatedMessageFactory;
import org.apache.ignite.internal.raft.service.SimulatedPersistenceService;
import org.apache.ignite.internal.raft.service.TimeServiceImpl;

/**
 *
 */
public class RaftWorker extends Thread {
    /** */
    private final BlockingQueue<WorkerAction> msgQueue = new LinkedBlockingDeque<>();

    /** */
    private final RaftReplicator replicator;

    /** */
    private volatile boolean done;

    /** */
    private final SimulatedPersistenceService pSvc;

    /** */
    private final StateMachine sm;

    /** */
    private final BiConsumer<UUID, RaftMessage> msgSink;

    /**
     */
    public RaftWorker(
        SimulatedPersistenceService pSvc,
        StateMachine sm,
        BiConsumer<UUID, RaftMessage> msgSink
    ) {
        this.pSvc = pSvc;
        this.sm = sm;
        this.msgSink = msgSink;

        replicator = new RaftReplicator(
            new ReplicatorConfig()
                .setElectionTimeout(5000)
                .setAppendEntriesBatchSize(10),
            pSvc,
            new TimeServiceImpl(System::currentTimeMillis, new Random()),
            new SimulatedMessageFactory()
        );
    }

    /**
     */
    public void terminate() throws InterruptedException {
        done = true;

        join();
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try {
            ReplicatorRole role = ReplicatorRole.FOLLOWER;
            long commitIdx = pSvc.hardState().commitIndex();
            UUID locId = pSvc.confState().localId();
            long term = pSvc.hardState().term();

            while (!done) {
                WorkerAction polled = msgQueue.poll(100, TimeUnit.MILLISECONDS);

                if (polled == null)
                    replicator.onTick();
                else {
                    if (polled instanceof MessageReceived) {
                        MessageReceived msg = (MessageReceived)polled;

                        replicator.step(msg.from(), msg.message());
                    }
                    else if (polled instanceof Propose) {
                        Propose p = (Propose)polled;

                        try {
                            AppendReceipt r = replicator.appendEntries(p.o);

                            p.future().complete(r);
                        }
                        catch (NotLeaderException e) {
                            p.future().completeExceptionally(e);
                        }
                    }
                }

                if (replicator.hasProgress()) {
                    Progress p = replicator.progress();

                    long now = System.currentTimeMillis();

                    if (p.hardState() != null) {
                        pSvc.hardState(p.hardState());

                        if (term != p.hardState().term()) {
                            System.out.println(locId + "@" + now + ": term " + term + " -> " + p.hardState().term());

                            term = p.hardState().term();
                        }
                    }

                    if (p.logEntries() != null)
                        pSvc.appendEntries(p.logEntries());

                    if (role != p.role()) {
                        System.out.println(locId + "@" + now + ": Role change " + role + " -> " + p.role());

                        role = p.role();
                    }

                    if (p.messages() != null) {
                        for (OutgoingMessage msg : p.messages())
                            msgSink.accept(msg.destinationPeerId(), msg.message());
                    }

                    if (p.hardState() != null && p.hardState().commitIndex() != commitIdx) {
                        LogEntry[] entries = new LogEntry[(int)(p.hardState().commitIndex() - commitIdx)];

                        pSvc.fetchEntries(commitIdx + 1, entries);

                        for (LogEntry entry : entries)
                            sm.apply(entry);

                        commitIdx = p.hardState().commitIndex();
                    }
                }
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     */
    public void onMessage(UUID from, RaftMessage msg) {
        msgQueue.add(new MessageReceived(from, msg));
    }

    /**
     */
    public CompletableFuture<AppendReceipt> propose(Object o) {
        Propose p = new Propose(o);

        msgQueue.add(p);

        return p.future();
    }

    /**
     */
    private abstract static class WorkerAction {

    }

    /**
     */
    private static class MessageReceived extends WorkerAction {
        /** */
        private final UUID from;

        /** */
        private final RaftMessage msg;

        /**
         */
        private MessageReceived(UUID from, RaftMessage msg) {
            this.from = from;
            this.msg = msg;
        }

        /**
         */
        private UUID from() {
            return from;
        }

        /**
         */
        private RaftMessage message() {
            return msg;
        }
    }

    /**
     */
    private static class Propose extends WorkerAction {
        /** */
        private final Object o;

        /** */
        private final CompletableFuture<AppendReceipt> f = new CompletableFuture<>();

        /**
         */
        public Propose(Object o) {
            this.o = o;
        }

        /**
         */
        private CompletableFuture<AppendReceipt> future() {
            return f;
        }
    }
}
