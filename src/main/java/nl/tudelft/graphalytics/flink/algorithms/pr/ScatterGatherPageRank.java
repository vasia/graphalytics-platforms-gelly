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

package nl.tudelft.graphalytics.flink.algorithms.pr;

import nl.tudelft.graphalytics.domain.algorithms.AlgorithmParameters;
import nl.tudelft.graphalytics.domain.algorithms.PageRankParameters;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeJoinFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;

public class ScatterGatherPageRank<K> implements GraphAlgorithm<K, Double, Double, DataSet<Vertex<K, Double>>> {

	private final double beta;
	private final int maxIterations;
	private final long numberOfVertices;

	public ScatterGatherPageRank(AlgorithmParameters params, long numVertices) {
		PageRankParameters prParams = (PageRankParameters)params;
		beta = prParams.getDampingFactor();
		maxIterations = prParams.getNumberOfIterations();
		numberOfVertices = numVertices;
	}

	@Override
	public DataSet<Vertex<K, Double>> run(Graph<K, Double, Double> network) throws Exception {

		DataSet<Tuple2<K, Long>> vertexOutDegrees = network.outDegrees();

		Graph<K, Double, Double> networkWithWeights = network
				.joinWithEdgesOnSource(vertexOutDegrees, new InitWeights());

		return networkWithWeights.runScatterGatherIteration(new VertexRankUpdater<K>(beta, numberOfVertices),
				new RankMessenger<K>(numberOfVertices), maxIterations)
				.getVertices();
	}

	/**
	 * Function that updates the rank of a vertex by summing up the partial
	 * ranks from all incoming messages and then applying the dampening formula.
	 */
	@SuppressWarnings("serial")
	public static final class VertexRankUpdater<K> extends VertexUpdateFunction<K, Double, Double> {

		private final double beta;
		private final long numVertices;
		
		public VertexRankUpdater(double beta, long numberOfVertices) {
			this.beta = beta;
			this.numVertices = numberOfVertices;
		}

		@Override
		public void updateVertex(Vertex<K, Double> vertex, MessageIterator<Double> inMessages) {
			double rankSum = 0.0;
			for (double msg : inMessages) {
				rankSum += msg;
			}

			// apply the dampening factor / random jump
			double newRank = (beta * rankSum) + (1 - beta) / numVertices;
			setNewVertexValue(newRank);
		}
	}

	/**
	 * Distributes the rank of a vertex among all target vertices according to
	 * the transition probability, which is associated with an edge as the edge
	 * value.
	 */
	@SuppressWarnings("serial")
	public static final class RankMessenger<K> extends MessagingFunction<K, Double, Double, Double> {

		private final long numVertices;

		public RankMessenger(long numberOfVertices) {
			this.numVertices = numberOfVertices;
		}

		@Override
		public void sendMessages(Vertex<K, Double> vertex) {
			if (getSuperstepNumber() == 1) {
				// initialize vertex ranks
				vertex.setValue(new Double(1.0 / numVertices));
			}

			for (Edge<K, Double> edge : getEdges()) {
				sendMessageTo(edge.getTarget(), vertex.getValue() * edge.getValue());
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class InitWeights implements EdgeJoinFunction<Double, Long> {

		public Double edgeJoin(Double edgeValue, Long inputValue) {
			return edgeValue / (double) inputValue;
		}
	}

}
