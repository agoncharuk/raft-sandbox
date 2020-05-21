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

/**
 * Exception thrown by a node if entries are proposed to replicator not in {@code LEADER} state.
 */
public class NotLeaderException extends RecoverableException {
    /** */
    private final UUID leaderId;

    /**
     * @param leaderId Leader ID, if known, {@code null} otherwise.
     */
    public NotLeaderException(String msg, UUID leaderId) {
        super(msg);

        this.leaderId = leaderId;
    }

    /**
     * @return Leader ID, if known.
     */
    public UUID leaderId() {
        return leaderId;
    }
}
