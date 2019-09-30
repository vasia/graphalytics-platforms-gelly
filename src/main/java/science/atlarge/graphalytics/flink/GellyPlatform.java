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

package science.atlarge.graphalytics.flink;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import science.atlarge.graphalytics.domain.algorithms.Algorithm;
import science.atlarge.graphalytics.domain.algorithms.AlgorithmParameters;
import science.atlarge.graphalytics.domain.benchmark.BenchmarkRun;
import science.atlarge.graphalytics.domain.graph.FormattedGraph;
import science.atlarge.graphalytics.domain.graph.LoadedGraph;
import science.atlarge.graphalytics.domain.graph.PropertyList;
import science.atlarge.graphalytics.domain.graph.PropertyType;
import science.atlarge.graphalytics.execution.AbstractPlatform;
import science.atlarge.graphalytics.execution.BenchmarkRunSetup;
import science.atlarge.graphalytics.execution.PlatformExecutionException;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.execution.RuntimeSetup;
import science.atlarge.graphalytics.flink.algorithms.bfs.ScatterGatherBFS;
import science.atlarge.graphalytics.flink.algorithms.cdlp.LabelPropagation;
import science.atlarge.graphalytics.flink.algorithms.lcc.LocalClusteringCoefficient;
import science.atlarge.graphalytics.flink.algorithms.pr.ScatterGatherPageRank;
import science.atlarge.graphalytics.flink.algorithms.sssp.ScatterGatherSSSP;
import science.atlarge.graphalytics.flink.algorithms.wcc.ScatterGatherConnectedComponents;
import science.atlarge.graphalytics.report.result.BenchmarkMetrics;

import java.util.HashMap;
import java.util.Map;

public class GellyPlatform extends AbstractPlatform {

	private final ExecutionEnvironment remoteEnv;

	private static final Logger LOG = LogManager.getLogger();
	private static final String HDFS_DIRECTORY = "/graphalytics";
	private static final String GELLY_PROPERTIES_FILE = "gelly.properties";
	private static final String HDFS_HOST_KEY = "gelly.hdfs.host";
	private static final String JOB_MANAGER_IPC_ADDRESS_KEY = "gelly.job.manager.address";
	private static final String JOB_MANAGER_IPC_PORT_KEY = "gelly.job.manager.port";
	private static final String HADOOP_CONFIG_KEY = "hadoop.config.home";
	private static final String GRAPHALYTICS_GELLY_JAR_KEY = "gelly.graphalytics.jar";
	private static final String GELLY_PARALLELISM_KEY = "gelly.job.parallelism";
	private static final String GELLY_TASKMANAGER_MEMORY_KEY = "gelly.job.taskmanager.memory";

	private final String hdfsHost;
	private final String jobManagerAddress;
	private final int jobManagerPort;
	private final String hadoopConfig;
	private final String gellyJarPath;
	private final int parallelism;
	private final int taskManagerMemory;

	private Map<String, Tuple2<String, String>> graphPaths = new HashMap<>();
	private PropertiesConfiguration config;

	public GellyPlatform() {
		setup();

		// read configuration parameters
		hdfsHost = config.getString(HDFS_HOST_KEY);
		jobManagerAddress = config.getString(JOB_MANAGER_IPC_ADDRESS_KEY);
		jobManagerPort = config.getInt(JOB_MANAGER_IPC_PORT_KEY);
		hadoopConfig = config.getString(HADOOP_CONFIG_KEY);
		gellyJarPath = config.getString(GRAPHALYTICS_GELLY_JAR_KEY);
		parallelism = config.getInt(GELLY_PARALLELISM_KEY);
		taskManagerMemory  =config.getInt(GELLY_TASKMANAGER_MEMORY_KEY);

		// setup Flink configuration
		org.apache.flink.configuration.Configuration clientConfiguration = new org.apache.flink.configuration.Configuration();
		clientConfiguration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, jobManagerAddress);
		clientConfiguration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerPort);
		clientConfiguration.setString(ConfigConstants.PATH_HADOOP_CONFIG, hadoopConfig);
		clientConfiguration.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, taskManagerMemory);

		remoteEnv = ExecutionEnvironment.createRemoteEnvironment(jobManagerAddress, jobManagerPort, clientConfiguration, gellyJarPath);
		remoteEnv.setParallelism(parallelism);

	}

	private void setup() {
		try {
			config = new PropertiesConfiguration(GELLY_PROPERTIES_FILE);
		} catch (ConfigurationException e) {
				e.printStackTrace();
			LOG.info("Could not find or load gelly.properties.");
			config = new PropertiesConfiguration();
		}
	}

	@Override
	public void verifySetup() {

	}

	@Override
	public LoadedGraph loadGraph(FormattedGraph graph) {
		Path vertexPath = new Path(graph.getVertexFilePath());
		Path edgePath = new Path(graph.getEdgeFilePath());
		Path hdfsVertexPath = new Path(HDFS_DIRECTORY + "/input/" + graph.getName() + ".v");
		Path hdfsEdgePath = new Path(HDFS_DIRECTORY + "/input/" + graph.getName() + ".e");

		try {
			FileSystem fs = FileSystem.get(new Configuration());
			fs.copyFromLocalFile(vertexPath, hdfsVertexPath);
			fs.copyFromLocalFile(edgePath, hdfsEdgePath);
			fs.close();

			graphPaths.put(graph.getName(), new Tuple2<>(
					hdfsVertexPath.toUri().getPath(), hdfsEdgePath.toUri().getPath()));
		} catch (Exception e) {
			LOG.error("*** ERROR while uploading the graph: " + e.getMessage());
		}
		return new LoadedGraph(graph, vertexPath.toString(), edgePath.toString());
	}

	@Override
	public void prepare(RunSpecification runSpecification) {

	}

	@Override
	public void startup(RunSpecification runSpecification) {

	}

	@Override
	public void run(RunSpecification runSpecification) throws PlatformExecutionException {
		BenchmarkRun benchmarkRun = runSpecification.getBenchmarkRun();
		BenchmarkRunSetup benchmarkRunSetup = runSpecification.getBenchmarkRunSetup();
		RuntimeSetup runtimeSetup = runSpecification.getRuntimeSetup();

		Algorithm algo = benchmarkRun.getAlgorithm();
		final FormattedGraph input = runtimeSetup.getLoadedGraph().getFormattedGraph();

		AlgorithmParameters parameters = benchmarkRun.getAlgorithmParameters();
		boolean isDirected = input.isDirected();

		boolean hasEdgeValues = false;

		// check if edges have values
		PropertyList edgeProps = input.getEdgeProperties();
		if (edgeProps.size() > 0 ) {
			if (edgeProps.get(0).getType().equals(PropertyType.REAL)) {
				hasEdgeValues = true;
			}
		}

		String outputPath = hdfsHost + HDFS_DIRECTORY + "/output/"
				+ input.getName() + "-" + algo.getAcronym();
		Tuple2<String, String> inputPaths = graphPaths.get(input.getName());
		String vertexPath = hdfsHost + inputPaths.f0;
		String edgesPath = hdfsHost + inputPaths.f1;

		GellyJob job;

		switch (algo.getAcronym()) {
			case "BFS": job = new GellyJob<>(remoteEnv, vertexPath, edgesPath, outputPath,
					new ScatterGatherBFS(parameters), hasEdgeValues);
				break;
			case "CDLP": job = new GellyJob<>(remoteEnv, vertexPath, edgesPath, outputPath,
					new LabelPropagation(parameters, isDirected), hasEdgeValues);
				break;
			case "LCC": job = new GellyJob<>(remoteEnv, vertexPath, edgesPath, outputPath,
					new LocalClusteringCoefficient(isDirected), hasEdgeValues);
				break;
			case "PR": job = new GellyJob<>(remoteEnv, vertexPath, edgesPath, outputPath,
					new ScatterGatherPageRank(parameters), hasEdgeValues);
				break;
			case "SSSP": job = new GellyJob<>(remoteEnv, vertexPath, edgesPath, outputPath,
					new ScatterGatherSSSP(parameters), hasEdgeValues);
				break;
			case "WCC": job = new GellyJob<>(remoteEnv, vertexPath, edgesPath, outputPath,
					new ScatterGatherConnectedComponents(isDirected), hasEdgeValues);
				break;
			default: throw new PlatformExecutionException("Algorithm " + algo.getAcronym() + " is not supported!");
		}

		try {
			job.runJob();
		} catch (Exception e) {
			e.printStackTrace();
			throw new PlatformExecutionException("Gelly job failed: " + e.getMessage());
		}
	}

	@Override
	public BenchmarkMetrics finalize(RunSpecification runSpecification) {
		return null;
	}

	@Override
	public void terminate(RunSpecification runSpecification) {

	}

	@Override
	public void deleteGraph(LoadedGraph loadedGraph) {

	}

	@Override
	public String getPlatformName() {
		return "Gelly";
	}
}
