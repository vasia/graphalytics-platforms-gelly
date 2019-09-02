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

package science.atlarge.graphalytics.flink.algorithms.bfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.utils.VertexToTuple2Map;
import org.apache.flink.types.NullValue;
import science.atlarge.graphalytics.domain.algorithms.AlgorithmParameters;
import science.atlarge.graphalytics.domain.algorithms.BreadthFirstSearchParameters;

public class ScatterGatherBFS implements GraphAlgorithm<Long, NullValue, NullValue, DataSet<Tuple2<Long, Long>>> {

	private final Long srcVertexId;
	private final Integer maxIterations;

	/**
	 * @param srcVertexId The ID of the source vertex.
	 * @param maxIterations The maximum number of iterations to run.
	 */
	public ScatterGatherBFS(Long srcVertexId, Integer maxIterations) {
		this.srcVertexId = srcVertexId;
		this.maxIterations = maxIterations;
	}

	public ScatterGatherBFS(AlgorithmParameters params) {
		BreadthFirstSearchParameters bfsParams = (BreadthFirstSearchParameters) params;
		srcVertexId = bfsParams.getSourceVertex();
		maxIterations = 100;
	}

	@Override
	public DataSet<Tuple2<Long, Long>> run(Graph<Long, NullValue, NullValue> input) {

		return input.mapVertices(new InitVerticesMapper(srcVertexId))
				.runScatterGatherIteration(
					new MinDistanceMessenger(),
					new VertexDistanceUpdater(),
					maxIterations)
				.getVertices().map(new VertexToTuple2Map<Long, Long>());
	}

	@SuppressWarnings("serial")
	public static final class InitVerticesMapper implements MapFunction<Vertex<Long, NullValue>, Long> {

		private Long srcVertexId;

		public InitVerticesMapper(Long srcId) {
			this.srcVertexId = srcId;
		}

		public Long map(Vertex<Long, NullValue> value) {
			if (value.f0.equals(srcVertexId)) {
				return 0l;
			} else {
				return Long.MAX_VALUE;
			}
		}
	}

	/**
	 * Function that updates the value of a vertex by picking the minimum
	 * distance from all incoming messages.
	 *
	 */
	@SuppressWarnings("serial")
	public static final class VertexDistanceUpdater extends GatherFunction<Long, Long, Long> {

		@Override
		public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) {

			long minDistance = Long.MAX_VALUE;

			for (long msg : inMessages) {
				if (msg < minDistance) {
					minDistance = msg;
				}
			}

			if (vertex.getValue() > minDistance) {
				setNewVertexValue(minDistance);
			}
		}
	}

	/**
	 * Distributes the minimum distance associated with a given vertex among all
	 * the target vertices summed up with the edge's value.
	 *
	 */
	@SuppressWarnings("serial")
	public static final class MinDistanceMessenger extends ScatterFunction<Long, Long, Long, NullValue> {

		@Override
		public void sendMessages(Vertex<Long, Long> vertex) {
			if (vertex.getValue() < Long.MAX_VALUE) {
				sendMessageToAllNeighbors(vertex.getValue() + 1);
			}
		}
	}

}
