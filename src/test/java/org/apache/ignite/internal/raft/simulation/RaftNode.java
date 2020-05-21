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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.raft.AppendReceipt;
import org.apache.ignite.internal.raft.message.RaftMessage;
import org.apache.ignite.internal.raft.service.SimulatedPersistenceService;

/**
 *
 */
public class RaftNode {
    /** */
    private final SimulatedPersistenceService pSvc;

    /** */
    private final StateMachine sm;

    /** */
    private final BiConsumer<UUID, RaftMessage> sink;

    /** */
    private volatile RaftWorker rw;

    /**
     */
    public RaftNode(SimulatedPersistenceService pSvc, StateMachine sm, BiConsumer<UUID, RaftMessage> sink) {
        this.pSvc = pSvc;
        this.sm = sm;
        this.sink = sink;
    }

    /**
     */
    public void start() {
        if (rw == null) {
            System.out.println("Starting " + pSvc.confState().localId());

            rw = new RaftWorker(pSvc, sm, sink);
            rw.start();
        }
        else
            System.out.println("Node is already running: " + pSvc.confState().localId());
    }

    /**
     */
    public void stop() throws InterruptedException {
        if (rw != null) {
            rw.terminate();
            rw = null;

            System.out.println("Stopped " + pSvc.confState().localId());
        }
        else
            System.out.println("Node is not running");
    }

    /**
     */
    public boolean running() {
        return rw != null;
    }

    /**
     */
    public void onMessage(UUID src, RaftMessage msg) {
        RaftWorker rw0 = rw;

        // Race is fine here because RAFT is tolerable for message loss.
        if (rw0 != null)
            rw0.onMessage(src, msg);
    }

    /**
     */
    public CompletableFuture<AppendReceipt> propose(HashMapWriteCommand writeCmd) {
        if (rw == null)
            throw new IllegalStateException();

        return rw.propose(writeCmd);
    }
}
