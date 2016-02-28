/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nl.tudelft.graphalytics.flink.algorithms.wcc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.types.NullValue;

public class GSAConnectedComponents<K> implements GraphAlgorithm<K, Long, NullValue, DataSet<Vertex<K, Long>>> {

	private final Integer maxIterations;
	private final boolean isDirected; 

	/**
	 * @param maxIterations The maximum number of iterations to run.
	 * @param directed true if the input graph is directed.
	 */
	public GSAConnectedComponents(Integer maxIterations, boolean directed) {
		this.maxIterations = maxIterations;
		this.isDirected = directed;
	}

	@Override
	public DataSet<Vertex<K, Long>> run(Graph<K, Long, NullValue> graph) throws Exception {

		if (isDirected) {
			graph = graph.getUndirected();
		}

		return graph.runGatherSumApplyIteration(
				new GatherNeighborIds(), new SelectMinId(), new UpdateComponentId<K>(),
				maxIterations).getVertices();
	}

	// --------------------------------------------------------------------------------------------
	//  Connected Components UDFs
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("serial")
	private static final class GatherNeighborIds extends GatherFunction<Long, NullValue, Long> {

		public Long gather(Neighbor<Long, NullValue> neighbor) {
			return neighbor.getNeighborValue();
		}
	};

	@SuppressWarnings("serial")
	private static final class SelectMinId extends SumFunction<Long, NullValue, Long> {

		public Long sum(Long newValue, Long currentValue) {
			return Math.min(newValue, currentValue);
		}
	};

	@SuppressWarnings("serial")
	private static final class UpdateComponentId<K> extends ApplyFunction<K, Long, Long> {

		public void apply(Long summedValue, Long origValue) {
			if (summedValue < origValue) {
				setResult(summedValue);
			}
		}
	}

}
