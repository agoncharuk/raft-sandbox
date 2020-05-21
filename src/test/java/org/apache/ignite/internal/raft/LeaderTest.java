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
import org.apache.ignite.internal.raft.service.ConfigurationState;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 *
 */
public class LeaderTest {
    /**
     */
    @Test
    public void testNextIndexNotAffectedByReorderedRejectReply1() {
        UUID[] g = new UUID[] {UUID.randomUUID(), UUID.randomUUID()};

        UUID node = g[1];
        Leader leader = new Leader(1, new ConfigurationState(g[0], g), 11);

        RemoteFollowerState follower = leader.remoteFollower(node);

        follower.updateNextEntries(true, 3, 0, 0);
        follower.updateNextEntries(false, 0, 0, 5);

        assertEquals(4, follower.nextIndex());
    }

    /**
     */
    @Test
    public void testNextIndexNotAffectedByReorderedRejectReply2() {
        UUID[] g = new UUID[] {UUID.randomUUID(), UUID.randomUUID()};

        UUID node = g[1];
        Leader leader = new Leader(1, new ConfigurationState(g[0], g), 11);

        RemoteFollowerState follower = leader.remoteFollower(node);

        follower.updateNextEntries(false, 0, 0, 5);
        follower.updateNextEntries(false, 0, 0, 7);

        assertEquals(5, follower.nextIndex());
    }

    // TODO test commit index group size 1

    /**
     */
    @Test
    public void testEffectiveCommitIndexGroupSize2() {
        UUID[] g = group(2);
        Leader leader = new Leader(1, new ConfigurationState(g[0], g), 11);

        leader.remoteFollower(g[1]).updateNextEntries(true, 2, 0, 0);

        assertEquals(2, leader.effectiveCommitIndex());

        leader.remoteFollower(g[1]).updateNextEntries(true, 3, 0, 0);

        assertEquals(3, leader.effectiveCommitIndex());
    }

    /**
     */
    @Test
    public void testEffectiveCommitIndexGroupSize3() {
        UUID[] g = group(3);

        Leader leader = new Leader(1, new ConfigurationState(g[0], g), 11);

        leader.remoteFollower(g[1]).updateNextEntries(true, 2, 0, 0);

        assertEquals(2, leader.effectiveCommitIndex());

        leader.remoteFollower(g[2]).updateNextEntries(true, 3, 0, 0);

        assertEquals(3, leader.effectiveCommitIndex());
    }

    /**
     */
    @Test
    public void testEffectiveCommitIndexGroupSize4() {
        UUID[] g = group(4);
        Leader leader = new Leader(1, new ConfigurationState(g[0], g), 11);


        leader.remoteFollower(g[1]).updateNextEntries(true, 2, 0, 0);

        assertEquals(0, leader.effectiveCommitIndex());

        leader.remoteFollower(g[2]).updateNextEntries(true, 3, 0, 0);

        assertEquals(2, leader.effectiveCommitIndex());

        leader.remoteFollower(g[3]).updateNextEntries(true, 3, 0, 0);

        assertEquals(3, leader.effectiveCommitIndex());
    }

    /**
     */
    @Test
    public void testEffectiveCommitIndexGroupSize5() {
        UUID[] g = group(5);
        Leader leader = new Leader(1, new ConfigurationState(g[0], g), 11);

        leader.remoteFollower(g[1]).updateNextEntries(true, 4, 0, 0);

        assertEquals(0, leader.effectiveCommitIndex());

        leader.remoteFollower(g[2]).updateNextEntries(true, 2, 0, 0);

        assertEquals(2, leader.effectiveCommitIndex());

        leader.remoteFollower(g[3]).updateNextEntries(true, 3, 0, 0);

        assertEquals(3, leader.effectiveCommitIndex());

        leader.remoteFollower(g[4]).updateNextEntries(true, 4, 0, 0);

        assertEquals(4, leader.effectiveCommitIndex());
    }

    /**
     */
    private static UUID[] group(int size) {
        UUID[] res = new UUID[size];

        for (int i = 0; i < size; i++)
            res[i] = UUID.randomUUID();

        return res;
    }
}
