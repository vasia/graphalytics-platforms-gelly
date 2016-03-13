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

package nl.tudelft.graphalytics.flink.algorithms.lcc;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.VertexJoinFunction;
import org.apache.flink.graph.utils.Tuple2ToVertexMap;
import org.apache.flink.graph.utils.VertexToTuple2Map;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.HashSet;

@SuppressWarnings("serial")
public class LocalClusteringCoefficient implements
	GraphAlgorithm<Long, Double, NullValue, DataSet<Tuple2<Long, Double>>> {

	private final Boolean directed;

	public LocalClusteringCoefficient(boolean directed) {
		this.directed = directed;
	}

	@Override
	public DataSet<Tuple2<Long, Double>> run(Graph<Long, Double, NullValue> graph) throws Exception {

		DataSet<Edge<Long, NullValue>> edges = graph.getEdges();

		// get all neighbors and attach as vertex value
		DataSet<Edge<Long, NullValue>> allEdges = edges;
		if (!directed) {
			allEdges = graph.getUndirected().getEdges(); 
		}

		DataSet<Vertex<Long, HashSet<Long>>> verticesWithNeighbors = allEdges.map(
				new MapFunction<Edge<Long,NullValue>, Tuple2<Long, HashSet<Long>>>() {
					public Tuple2<Long, HashSet<Long>> map(Edge<Long, NullValue> edge) {
						HashSet<Long> neighbors = new HashSet<>();
						neighbors.add(edge.f1);
						return new Tuple2<>(edge.f0, neighbors);
					}
				}).groupBy(0).reduce(new ReduceFunction<Tuple2<Long,HashSet<Long>>>() {
					public Tuple2<Long, HashSet<Long>> reduce(
							Tuple2<Long, HashSet<Long>> set1,
							Tuple2<Long, HashSet<Long>> set2) {
						set1.f1.addAll(set2.f1);
						return set1;
					}
				}).map(new Tuple2ToVertexMap<Long, HashSet<Long>>());

		DataSet<Tuple3<Long, Long, Long>> candidates = verticesWithNeighbors.flatMap(
				new FlatMapFunction<Vertex<Long,HashSet<Long>>, Tuple3<Long, Long, Long>>() {
					public void flatMap(Vertex<Long, HashSet<Long>> vertex,
							Collector<Tuple3<Long, Long, Long>> out) {

			    Object[] neighbors = vertex.f1.toArray();
				Tuple3<Long, Long, Long> outTuple = new Tuple3<>();
				outTuple.setField(vertex.f0, 0);

				for (int i = 0; i < neighbors.length; i++) {
					for (int j = 0; j < neighbors.length; j++) {
						if (i != j) {
							outTuple.setField((long)neighbors[i], 1);
							outTuple.setField((long)neighbors[j], 2);
							out.collect(outTuple);
						}
					}
				}
			}
		});

		DataSet<Tuple2<Long, Long>> verticesWithNumLinks = 
				candidates.join(edges).where(1, 2).equalTo(0, 1)
				.<Tuple1<Long>>projectFirst(0).map(new MapFunction<Tuple1<Long>, Tuple2<Long, Long>>() {
					public Tuple2<Long, Long> map(Tuple1<Long> value) {
						return new Tuple2<>(value.f0, 1l);
					}
				}).groupBy(0).sum(1);
		
		DataSet<Tuple2<Long, Integer>> verticesWithSize = verticesWithNeighbors.map(
				new MapFunction<Vertex<Long,HashSet<Long>>, Tuple2<Long, Integer>>() {

					public Tuple2<Long, Integer> map(Vertex<Long, HashSet<Long>> vertex) {
						return new Tuple2<>(vertex.getId(), vertex.getValue().size());
					}
		});

		DataSet<Tuple2<Long, Double>> result = verticesWithNumLinks.join(verticesWithSize)
				.where(0).equalTo(0).with(
						new FlatJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Integer>, Tuple2<Long, Double>>() {
							public void join(Tuple2<Long, Long> vertexWithNumLinks,
									Tuple2<Long, Integer> vertexWithSetSize,
									Collector<Tuple2<Long, Double>> out) {
								out.collect(new Tuple2<>(vertexWithNumLinks.f0,
										(double)vertexWithNumLinks.f1 /
										(double)(vertexWithSetSize.f1 * (vertexWithSetSize.f1 - 1))));
							}
				});

		return graph.joinWithVertices(result, new VertexJoinFunction<Double, Double>() {
			public Double vertexJoin(Double vertexValue, Double inputValue) {
				return inputValue;
			}
		}).getVertices().map(new VertexToTuple2Map<Long, Double>());

	}
}
