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

package nl.tudelft.graphalytics.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.types.NullValue;

/**
 *
 * @param <T> output type (vertex value type after running the algorithm)
 */
public class GellyJob<T> {

	private final String vertexInputPath;
	private final String edgesInputPath;
	private final String outputPath;
	private final boolean hasEdgeValue;
	private final GraphAlgorithm<Long, NullValue, ?, DataSet<Tuple2<Long, T>>> algorithm;

	public GellyJob(
			String vPath, String ePath, String outputPath,
			GraphAlgorithm algorithm, boolean edgeValue) {

		this.vertexInputPath = vPath;
		this.edgesInputPath = ePath;
		this.outputPath = outputPath;
		this.algorithm = algorithm;
		this.hasEdgeValue = edgeValue;
	}

	/**
	 * Parse the input vertices and edges,
	 * execute the Gelly job, and write back the result
	 */
	public void runJob() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		if (hasEdgeValue) {
			Graph<Long, NullValue, Double> graph =
					Graph.fromCsvReader(vertexInputPath, edgesInputPath, env)
							.fieldDelimiterEdges(" ")
							.edgeTypes(Long.class, Double.class);
			graph.run((GraphAlgorithm<Long, NullValue, Double, DataSet<Tuple2<Long, T>>>) algorithm)
					.writeAsText(outputPath);
			env.execute();
		}
		else {
			Graph<Long, NullValue, NullValue> graph =
					Graph.fromCsvReader(vertexInputPath, edgesInputPath, env)
							.fieldDelimiterEdges(" ")
							.keyType(Long.class);
			graph.run((GraphAlgorithm<Long, NullValue, NullValue, DataSet<Tuple2<Long, T>>>) algorithm)
					.writeAsText(outputPath);
			env.execute();
		}

	}
}
