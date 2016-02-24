package nl.tudelft.graphalytics.flink;

import nl.tudelft.graphalytics.domain.GraphFormat;

abstract class GellyJob {

	private String graphInputPath;
	private GraphFormat graphFormat;
	private String outputPath;

	public GellyJob(String inputPath, GraphFormat format, String outputPath) {
		this.graphInputPath = inputPath;
		this.graphFormat = format;
		this.outputPath = outputPath;
	}

}
