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
package org.tripatha.rdftriplestore;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;

import org.eclipse.rdf4j.query.QueryInterruptedException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.tripatha.RyaSailITBase;
import org.tripatha.store.StoreRdfConfiguration;

/**
 * A query past its time limit ends with RDF4J's {@link QueryInterruptedException} (the endpoint's
 * 503), whatever it is doing when the limit passes. RDF4J closes the query's iterations from a timer
 * thread, so the store's reads are closed while the query thread may be inside one (SCALE-STUDY.md
 * 4.28: on Accumulo that read threw instead, and the query failed with a plain RuntimeException).
 */
public class QueryTimeoutIT extends RyaSailITBase {
    /** Nodes, each linked to every node by each of three predicates. */
    private static final int NODES = 60;
    /**
     * A chain, so that each row looks the next pattern up in the store (a product of unrelated
     * patterns is read once and joined in memory): 60⁴ rows and about 216,000 lookups, which no
     * backend answers within the second the query gets.
     */
    private static final String CHAIN = "SELECT * WHERE { ?a <urn:p0> ?b . ?b <urn:p1> ?c . ?c <urn:p2> ?d }";

    private SailRepository repo;

    @AfterEach
    public void shutDown() {
        if (repo != null) {
            repo.shutDown();
        }
    }

    /** Batched joins: the store is read once per window of left rows. */
    @Test
    public void aBatchedQueryPastItsTimeLimitIsInterrupted() {
        interruptedEveryTime(newConf());
    }

    /**
     * RDF4J's own nested loop: a store read for every left row, so the limit passes while the query
     * thread is inside one.
     */
    @Test
    public void aNestedLoopQueryPastItsTimeLimitIsInterrupted() {
        final StoreRdfConfiguration conf = newConf();
        conf.setJoinBatchSize(1);
        interruptedEveryTime(conf);
    }

    private void interruptedEveryTime(final StoreRdfConfiguration conf) {
        repo = new SailRepository(newStore(conf, false));
        repo.init();
        try (RepositoryConnection conn = repo.getConnection()) {
            final StringBuilder data = new StringBuilder("INSERT DATA {");
            for (int p = 0; p < 3; p++) {
                for (int i = 0; i < NODES; i++) {
                    for (int j = 0; j < NODES; j++) {
                        data.append(" <urn:n").append(i).append("> <urn:p").append(p).append("> <urn:n").append(j).append("> .");
                    }
                }
            }
            conn.prepareUpdate(data.append(" }").toString()).execute();
        }
        assertTimeoutPreemptively(Duration.ofMinutes(3), () -> {
            for (int round = 0; round < 3; round++) {
                try (RepositoryConnection conn = repo.getConnection()) {
                    final TupleQuery query = conn.prepareTupleQuery(CHAIN);
                    query.setMaxExecutionTime(1);
                    assertThrows(QueryInterruptedException.class, () -> {
                        try (TupleQueryResult result = query.evaluate()) {
                            while (result.hasNext()) {
                                result.next();
                            }
                        }
                    }, "round " + round);
                }
            }
        });
    }
}
