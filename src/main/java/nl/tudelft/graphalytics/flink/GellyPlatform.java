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
