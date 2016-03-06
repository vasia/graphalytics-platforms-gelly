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

import nl.tudelft.graphalytics.AbstractPlatform;
import nl.tudelft.graphalytics.PlatformExecutionException;
import nl.tudelft.graphalytics.domain.Benchmark;
import nl.tudelft.graphalytics.domain.Graph;
import nl.tudelft.graphalytics.domain.PlatformBenchmarkResult;

public class GellyPlatform extends AbstractPlatform {

	@Override
	public void uploadGraph(Graph graph) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public PlatformBenchmarkResult executeAlgorithmOnGraph(Benchmark benchmark)
			throws PlatformExecutionException {
		// TODO check which algorithm and call the corresponding Gelly job
		// TODO retrieve the graph input path and specification from the graph object
		// and create the Gelly graph
		// TODO what are the parameters?!
		throw new PlatformExecutionException("No algorithms are supported for Gelly yet!");
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
