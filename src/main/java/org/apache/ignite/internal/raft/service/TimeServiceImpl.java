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

package org.apache.ignite.internal.raft.service;

import java.util.Random;

/**
 *
 */
public class TimeServiceImpl implements TimeService {
    /** */
    private final TimeSource timeSrc;

    /** */
    private final Random rnd;

    /**
     * @param timeSrc Time source delegate.
     * @param rnd Random, may be {@code null}.
     */
    public TimeServiceImpl(
        TimeSource timeSrc,
        Random rnd
    ) {
        this.timeSrc = timeSrc;
        this.rnd = rnd;
    }

    /** {@inheritDoc} */
    @Override public long nextRandomizedDeadline(long timeout, float variation) {
        float sample = rnd == null ? 0 : rnd.nextFloat();
        long now = currentTimeMillis();

        float multiplier = variation >= 1 ?
            1 + sample * (variation - 1) :
            variation + sample * (1 - variation);

        return now + (long)(timeout * multiplier);
    }

    /** {@inheritDoc} */
    @Override public long currentTimeMillis() {
        return timeSrc.currentTimeMillis();
    }
}
