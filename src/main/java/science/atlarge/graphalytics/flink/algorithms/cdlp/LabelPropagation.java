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

package science.atlarge.graphalytics.flink.algorithms.cdlp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.graph.utils.VertexToTuple2Map;
import org.apache.flink.types.NullValue;
import science.atlarge.graphalytics.domain.algorithms.AlgorithmParameters;
import science.atlarge.graphalytics.domain.algorithms.CommunityDetectionLPParameters;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

@SuppressWarnings("serial")
public class LabelPropagation implements GraphAlgorithm<Long, NullValue, NullValue, DataSet<Tuple2<Long, Long>>> {

	private final int maxIterations;
	private final boolean isDirected;

	public LabelPropagation(AlgorithmParameters params, boolean directed) {
		this.maxIterations = ((CommunityDetectionLPParameters)(params)).getMaxIterations();
		this.isDirected = directed;
	}

	@Override
	public DataSet<Tuple2<Long, Long>> run(Graph<Long, NullValue, NullValue> input) {

		Graph<Long, Long, NullValue> initializedInput =
				input.mapVertices(new InitVertexValues());

		if (isDirected) {
			initializedInput = initializedInput.getUndirected();
		}

		ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();
		parameters.setDirection(EdgeDirection.ALL);

		return initializedInput.runScatterGatherIteration(
					new SendNewLabelToNeighbors(),
					new UpdateVertexLabel(),
					maxIterations,
					parameters)
				.getVertices().map(new VertexToTuple2Map<>());
	}
	
	/**
	 * Function that updates the value of a vertex by adopting the most frequent
	 * label among its neighbors
	 */
	public static final class UpdateVertexLabel extends GatherFunction<Long, Long, Long> {
	
		public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) {
			Map<Long, Long> labelsWithFrequencies = new HashMap<>();
	
			long maxFrequency = 1L;
			long mostFrequentLabel = vertex.getValue();
	
			// store the labels with their frequencies
			for (java.lang.Long msg : inMessages) {
				if (labelsWithFrequencies.containsKey(msg)) {
					long currentFreq = labelsWithFrequencies.get(msg);
					labelsWithFrequencies.put(msg, currentFreq + 1);
				} else {
					labelsWithFrequencies.put(msg, 1L);
				}
			}

			// select the most frequent label: if two or more labels have the
			// same frequency, the vertex adopts the label with the smallest value
			for (Entry<Long, Long> entry : labelsWithFrequencies.entrySet()) {
				if (entry.getValue() == maxFrequency) {
					// check the label value to break ties
					if (entry.getKey().compareTo(mostFrequentLabel) < 0) {
						mostFrequentLabel = entry.getKey();
					}
				} else if (entry.getValue() > maxFrequency) {
					maxFrequency = entry.getValue();
					mostFrequentLabel = entry.getKey();
				}
			}
			setNewVertexValue(mostFrequentLabel);
		}
	}
	
	/**
	 * Sends the vertex label to all out-neighbors
	 */
	public static final class SendNewLabelToNeighbors extends ScatterFunction<Long, Long, Long, NullValue> {
	
		public void sendMessages(Vertex<Long, Long> vertex) {
			sendMessageToAllNeighbors(vertex.getValue());
		}
	}

	private static final class InitVertexValues implements MapFunction<Vertex<Long, NullValue>, Long> {
		public Long map(Vertex<Long, NullValue> vertex) {
			return vertex.getId();
		}
	}
}
