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

import java.util.HashMap;
import java.util.Map;

import nl.tudelft.graphalytics.domain.*;
import nl.tudelft.graphalytics.domain.algorithms.AlgorithmParameters;
import nl.tudelft.graphalytics.flink.algorithms.bfs.ScatterGatherBFS;
import nl.tudelft.graphalytics.flink.algorithms.cdlp.LabelPropagation;
import nl.tudelft.graphalytics.flink.algorithms.lcc.LocalClusteringCoefficient;
import nl.tudelft.graphalytics.flink.algorithms.pr.ScatterGatherPageRank;
import nl.tudelft.graphalytics.flink.algorithms.sssp.ScatterGatherSSSP;
import nl.tudelft.graphalytics.flink.algorithms.wcc.ScatterGatherConnectedComponents;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import nl.tudelft.graphalytics.AbstractPlatform;
import nl.tudelft.graphalytics.PlatformExecutionException;

public class GellyPlatform extends AbstractPlatform {

	private static String HDFS_DIRECTORY_KEY = "hadoop.hdfs.directory";
	private static String HDFS_DIRECTORY = "graphalytics";
	private Map<String, Tuple2<String, String>> graphPaths = new HashMap<>();

	@Override
	public void uploadGraph(Graph graph) throws Exception {
		Path vertexPath = new Path(graph.getVertexFilePath());
		Path edgePath = new Path(graph.getEdgeFilePath());
		Path hdfsVertexPath = new Path(HDFS_DIRECTORY + "/input/" + graph.getName() + ".vertices");
		Path hdfsEdgePath = new Path(HDFS_DIRECTORY + "/input/" + graph.getName() + ".edges");

		FileSystem fs = FileSystem.get(new Configuration());
		fs.copyFromLocalFile(vertexPath, hdfsVertexPath);
		fs.copyFromLocalFile(edgePath, hdfsEdgePath);
		fs.close();

		graphPaths.put(graph.getName(), new Tuple2<>(
				hdfsVertexPath.toUri().getPath(), hdfsEdgePath.toUri().getPath()));
	}

	@Override
	public PlatformBenchmarkResult executeAlgorithmOnGraph(Benchmark benchmark)
			throws PlatformExecutionException {

		Algorithm algo = benchmark.getAlgorithm();
		Graph input = benchmark.getGraph();
		AlgorithmParameters parameters = (AlgorithmParameters) benchmark.getAlgorithmParameters();
		boolean isDirected = input.isDirected();
		String outputPath = HDFS_DIRECTORY + "/output/" + input.getName() + "-" + algo.getName();
		Tuple2<String, String> inputPaths = graphPaths.get(input.getName());

		GellyJob job;

		switch (algo.getAcronym()) {
			case "BFS": job = new GellyJob(inputPaths.f0, inputPaths.f1, outputPath,
					new ScatterGatherBFS(parameters));
				break;
			case "CDLP": job = new GellyJob(inputPaths.f0, inputPaths.f1, outputPath,
					new LabelPropagation(parameters, isDirected));
				break;
			case "LCC": job = new GellyJob(inputPaths.f0, inputPaths.f1, outputPath,
					new LocalClusteringCoefficient());
				break;
			case "PR": job = new GellyJob(inputPaths.f0, inputPaths.f1, outputPath,
					new ScatterGatherPageRank<Long>(parameters, input.getNumberOfVertices()));
				break;
			case "SSSP": job = new GellyJob(inputPaths.f0, inputPaths.f1, outputPath,
					new ScatterGatherSSSP(parameters));
				break;
			case "WCC": job = new GellyJob(inputPaths.f0, inputPaths.f1, outputPath,
					new ScatterGatherConnectedComponents<Long>(isDirected));
				break;
			default: throw new PlatformExecutionException("Algorithm " + algo.getAcronym() + " is not supported!");
		}

		try {
			job.runJob();

			if(benchmark.isOutputRequired()){
				FileSystem fs = FileSystem.get(new Configuration());
				fs.copyToLocalFile(new Path(outputPath), new Path(benchmark.getOutputPath()));
				fs.close();
			}
		} catch (Exception e) {
			throw new PlatformExecutionException("Gelly job failed: " + e.getMessage());
		}

		return new PlatformBenchmarkResult(NestedConfiguration.empty());
	}

	@Override
	public void deleteGraph(String graphName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getName() {
		return "Gelly";
	}

}
