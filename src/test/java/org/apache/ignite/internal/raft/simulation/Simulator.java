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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.raft.AppendReceipt;
import org.apache.ignite.internal.raft.service.ConfigurationState;
import org.apache.ignite.internal.raft.service.HardState;
import org.apache.ignite.internal.raft.NotLeaderException;
import org.apache.ignite.internal.raft.message.RaftMessage;
import org.apache.ignite.internal.raft.service.SimulatedPersistenceService;

/**
 *
 */
public class Simulator {
    /** */
    private Map<UUID, RaftNode> nodes;

    /** */
    private UUID leader;

    /**
     */
    public void start() throws IOException {
        UUID[] grp = new UUID[5];
        for (int i = 0; i < grp.length; i++)
            grp[i] = UUID.randomUUID();

        nodes = new HashMap<>();

        for (UUID src : grp) {
            BiConsumer<UUID, RaftMessage> msgDispatcher = new BiConsumer<UUID, RaftMessage>() {
                @Override public void accept(UUID id, RaftMessage msg) {
                    RaftNode dst = nodes.get(id);

                    if (dst != null)
                        dst.onMessage(src, msg);
                }
            };

            RaftNode worker = new RaftNode(
                new SimulatedPersistenceService(
                    new HardState(1, null, 0),
                    new ConfigurationState(src, grp)),
                new HashMapStateMachine(src),
                msgDispatcher
            );

            nodes.put(src, worker);
        }

        System.out.println("Created group: " + Arrays.toString(grp));

        BufferedReader rdr = new BufferedReader(new InputStreamReader(System.in));

        while (!Thread.currentThread().isInterrupted()) {
            String line = rdr.readLine();

            String[] cmd = line.split(" ");

            if (cmd.length >= 1) {
                switch (cmd[0]) {
                    case "write": {
                        if (cmd.length >= 3)
                            proposeCommand(cmd[1], cmd[2]);

                        break;
                    }

                    case "start": {
                        if (cmd.length >= 2) {
                            try {
                                UUID uuid = UUID.fromString(cmd[1]);

                                RaftNode node = nodes.get(uuid);

                                if (node == null)
                                    System.out.println("Node not found: ");
                                else
                                    node.start();
                            }
                            catch (IllegalArgumentException e) {
                                System.out.println("Invalid UUID: " + cmd[1]);
                            }
                        }

                        break;
                    }

                    case "stop": {
                        if (cmd.length >= 2) {
                            try {
                                UUID uuid = UUID.fromString(cmd[1]);

                                RaftNode node = nodes.get(uuid);

                                if (node == null)
                                    System.out.println("Node not found: ");
                                else
                                    node.stop();
                            }
                            catch (InterruptedException e) {
                                System.out.println("Interrupted, will abort");

                                Thread.currentThread().interrupt();
                            }
                            catch (IllegalArgumentException e) {
                                System.out.println("Invalid UUID: " + cmd[1]);
                            }
                        }

                        break;
                    }

                    case "exit": {
                        for (RaftNode node : nodes.values()) {
                            try {
                                if (node.running())
                                    node.stop();
                            }
                            catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }

                        Thread.currentThread().interrupt();

                        break;
                    }

                    default:
                        System.out.println("Unknown command: " + line);
                }
            }
        }
    }

    /**
     */
    private void proposeCommand(String key, String val) {
        while (true) {
            if (leader == null) {
                for (Map.Entry<UUID, RaftNode> entry : nodes.entrySet()) {
                    if (entry.getValue().running()) {
                        leader = entry.getKey();

                        break;
                    }
                }

                if (leader == null) {
                    System.out.println("No nodes running");

                    return;
                }
            }

            try {
                CompletableFuture<AppendReceipt> fut = nodes.get(leader).propose(new HashMapWriteCommand(key, val));

                AppendReceipt receipt = fut.get();

                System.out.println("Will try to replicate: " + receipt);

                return;
            }
            catch (InterruptedException e) {
                System.out.println("Interrupted, aborting");

                Thread.currentThread().interrupt();

                return;
            }
            catch (IllegalStateException e) {
                leader = null;
            }
            catch (ExecutionException e) {
                if (e.getCause() instanceof NotLeaderException) {
                    NotLeaderException le = (NotLeaderException)e.getCause();

                    if (le.leaderId() != null)
                        leader = le.leaderId();
                    else {
                        System.out.println("Leader is unknown, try again later");

                        return;
                    }
                }
            }
        }
    }

    /**
     */
    public static void main(String[] args) throws Exception {
        new Simulator().start();
    }
}
