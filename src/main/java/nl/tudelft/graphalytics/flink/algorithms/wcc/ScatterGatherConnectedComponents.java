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
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.types.NullValue;

public class ScatterGatherConnectedComponents <K> implements
		GraphAlgorithm<K, Long, NullValue, DataSet<Vertex<K, Long>>> {

	private final Integer maxIterations;
	private final boolean isDirected;

	/**
	 * @param maxIterations The maximum number of iterations to run.
	 * @param directed true if the input graph is directed
	 */
	public ScatterGatherConnectedComponents(Integer maxIterations, Boolean directed) {
		this.maxIterations = maxIterations;
		this.isDirected = directed;
	}

	@Override
	public DataSet<Vertex<K, Long>> run(Graph<K, Long, NullValue> graph) throws Exception {

		if (isDirected) {
			graph = graph.getUndirected();
		}
		return graph.runScatterGatherIteration(
				new CCUpdater<K>(), new CCMessenger<K>(), maxIterations)
				.getVertices();
	}

	/**
	 * Updates the value of a vertex by picking the minimum neighbor ID out of all the incoming messages.
	 */
	@SuppressWarnings("serial")
	public static final class CCUpdater<K> extends VertexUpdateFunction<K, Long, Long> {

		@Override
		public void updateVertex(Vertex<K, Long> vertex, MessageIterator<Long> messages) throws Exception {
			long min = Long.MAX_VALUE;

			for (long msg : messages) {
				min = Math.min(min, msg);
			}
			if (min < vertex.getValue()) {
				setNewVertexValue(min);
			}
		}
	}

	/**
	 * Distributes the minimum ID associated with a given vertex among all the target vertices.
	 */
	@SuppressWarnings("serial")
	public static final class CCMessenger<K> extends MessagingFunction<K, Long, Long, NullValue> {

		@Override
		public void sendMessages(Vertex<K, Long> vertex) throws Exception {
			sendMessageToAllNeighbors(vertex.getValue());
		}
	}

}
