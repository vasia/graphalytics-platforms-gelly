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

package nl.tudelft.graphalytics.flink.algorithms.cdlp;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.types.NullValue;

@SuppressWarnings("serial")
public class LabelPropagation<K> implements GraphAlgorithm<K, Long, NullValue, DataSet<Vertex<K, Long>>> {

	private final int maxIterations;
	private final boolean isDirected;
	
	/**
	 * Creates a new Label Propagation algorithm instance.
	 * The algorithm converges when vertices no longer update their value
	 * or when the maximum number of iterations is reached.
	 * 
	 * @param maxIterations The maximum number of iterations to run.
	 */
	public LabelPropagation(int maxIterations, boolean directed) {
		this.maxIterations = maxIterations;
		this.isDirected = directed;
	}
	
	@Override
	public DataSet<Vertex<K, Long>> run(Graph<K, Long, NullValue> input) {

		if (isDirected) {
			input = input.getUndirected();
		}
		return input.runScatterGatherIteration(
				new UpdateVertexLabel<K>(), new SendNewLabelToNeighbors<K>(), maxIterations)
				.getVertices();
	}
	
	/**
	 * Function that updates the value of a vertex by adopting the most frequent
	 * label among its in-neighbors
	 */
	public static final class UpdateVertexLabel<K> extends VertexUpdateFunction<K, Long, Long> {
	
		public void updateVertex(Vertex<K, Long> vertex, MessageIterator<Long> inMessages) {
			Map<Long, Long> labelsWithFrequencies = new HashMap<>();
	
			long maxFrequency = 1;
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
			// same frequency, the node adopts the label with the smallest value
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
	public static final class SendNewLabelToNeighbors<K> extends MessagingFunction<K, Long, Long, NullValue> {
	
		public void sendMessages(Vertex<K, Long> vertex) {
			sendMessageToAllNeighbors(vertex.getValue());
		}
	}

}
