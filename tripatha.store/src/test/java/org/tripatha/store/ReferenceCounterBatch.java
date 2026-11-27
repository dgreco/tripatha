/*
 * Copyright 2026 David Greco
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tripatha.store;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.tripatha.api.domain.RyaStatement;
import org.tripatha.store.EvalStatsMutations.Counter;
import org.tripatha.store.EvalStatsMutations.CounterKey;
import org.tripatha.store.EvalStatsMutations.CounterPolicy;
import org.tripatha.store.spi.RowMutation;

/**
 * {@link EvalStatsMutations.CounterBatch} as it was before O35: a map of string keys, built by
 * {@link EvalStatsMutations#counterKeys}. The oracle {@code EvalStatsMutationsTest} compares the
 * byte-keyed batch with.
 */
final class ReferenceCounterBatch {
    private static final byte[] EMPTY = new byte[0];

    private final Map<CounterKey, long[]> sums = new HashMap<>();
    private byte[] label;
    private boolean labels;

    void add(final RyaStatement stmt, final long delta) {
        if (!labels) {
            final byte[] l = stmt.getColumnVisibility() == null ? EMPTY : stmt.getColumnVisibility();
            if (label == null) {
                label = l;
            } else if (!java.util.Arrays.equals(label, l)) {
                labels = true;
            }
        }
        for (final CounterKey key : EvalStatsMutations.counterKeys(stmt)) {
            sums.computeIfAbsent(key, k -> new long[1])[0] += delta;
        }
    }

    int size() {
        return sums.size();
    }

    void forEachSum(final java.util.function.ObjLongConsumer<CounterKey> action) {
        sums.forEach((key, sum) -> action.accept(key, sum[0]));
    }

    private Map<Counter, int[]> subjectCellsPerCounter() {
        final Map<Counter, int[]> cells = new HashMap<>();
        sums.forEach((key, sum) -> {
            if (sum[0] != 0 && CounterPolicy.isSubjectCounter(key.family())) {
                cells.computeIfAbsent(key.counter(), c -> new int[1])[0]++;
            }
        });
        return cells;
    }

    Map<Counter, int[]> cellsPerCounter() {
        final Map<Counter, int[]> cells = new HashMap<>();
        sums.forEach((key, sum) -> {
            if (sum[0] != 0) {
                cells.computeIfAbsent(key.counter(), c -> new int[1])[0]++;
            }
        });
        return cells;
    }

    List<RowMutation> drain(final CounterPolicy policy) {
        final Map<Counter, int[]> cells = policy == CounterPolicy.ALL || !labels ? null : subjectCellsPerCounter();
        final Map<String, RowMutation> byRow = new LinkedHashMap<>();
        for (final Map.Entry<CounterKey, long[]> e : sums.entrySet()) {
            final long sum = e.getValue()[0];
            final CounterKey key = e.getKey();
            if (sum == 0 || policy != CounterPolicy.ALL && sum == 1
                    && policy.skips(key, sum, cells == null || !CounterPolicy.isSubjectCounter(key.family()) ? 1
                            : cells.get(key.counter())[0])) {
                continue;
            }
            final RowMutation m = byRow.computeIfAbsent(key.row(),
                    row -> new RowMutation(row.getBytes(StandardCharsets.UTF_8)));
            m.addToCounter(key.family().getBytes(StandardCharsets.UTF_8),
                    key.graph().isEmpty() ? EMPTY : key.graph().getBytes(StandardCharsets.UTF_8),
                    key.visibility().getBytes(StandardCharsets.ISO_8859_1), sum);
        }
        sums.clear();
        label = null;
        labels = false;
        return new ArrayList<>(byRow.values());
    }
}
