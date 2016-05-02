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

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.*;
import org.apache.flink.graph.library.TriangleEnumerator;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class LCCWithTriangles implements
	GraphAlgorithm<Long, NullValue, NullValue, DataSet<Tuple2<Long, DoubleValue>>> {

	@Override
	public DataSet<Tuple2<Long, DoubleValue>> run(Graph<Long, NullValue, NullValue> graph) throws Exception {

		DataSet<Tuple3<Long, Long, Long>> triangles = graph.run(new TriangleEnumerator<Long, NullValue, NullValue>());

		DataSet<Tuple2<Long, LongValue>> extendedTriangles =
				triangles.flatMap(new FlatMapFunction<Tuple3<Long, Long, Long>, Tuple2<Long, LongValue>>() {
					private LongValue one = new LongValue(1);

					@Override
					public void flatMap(Tuple3<Long, Long, Long> t, Collector<Tuple2<Long, LongValue>> out) {
						out.collect(new Tuple2<>(t.f0, one));
						out.collect(new Tuple2<>(t.f1, one));
						out.collect(new Tuple2<>(t.f2, one));
					}
				});

		// count triangles per vertex
		DataSet<Tuple2<Long, LongValue>> verticesWithTriangleCounts = extendedTriangles.groupBy(0).sum(1);

		// get vertex degrees
		DataSet<Tuple2<Long, Long>> degrees = graph.getDegrees();

		// compute clustering coefficient
		DataSet<Tuple2<Long, DoubleValue>> result = verticesWithTriangleCounts.coGroup(degrees).where(0).equalTo(0)
				.with(new ComputeClusteringCoefficient());

		return result;
	}

	@FunctionAnnotation.ForwardedFieldsFirst("0")
	private static final class ComputeClusteringCoefficient implements
			CoGroupFunction<Tuple2<Long, LongValue>, Tuple2<Long, Long>, Tuple2<Long, DoubleValue>> {

		private DoubleValue cc = new DoubleValue();
		Tuple2<Long, DoubleValue> result = new Tuple2<>();

		@Override
		public void coGroup(Iterable<Tuple2<Long, LongValue>> vertexWithCount, Iterable<Tuple2<Long, Long>> vertexWithDegree,
							Collector<Tuple2<Long, DoubleValue>> out) throws Exception {

			long degree;
			long vertexID;
			long denominator = 0;
			cc.setValue(0);

			for (Tuple2<Long, Long> t : vertexWithDegree) {
				vertexID = t.f0;
				degree = t.f1 / 2;
				denominator = degree * (degree - 1);
				result.setField(vertexID, 0);
			}

			for (Tuple2<Long, LongValue> t: vertexWithCount) {
				if (denominator != 0) {
					cc.setValue((double) (2 * t.f1.getValue()) / (double) denominator);
				}
			}

			result.setField(cc, 1);
			out.collect(result);
		}
	}
}
