package Streaming.ReusableStream.util;

import Streaming.ReusableStream.ReusableDoublePipeline;

@FunctionalInterface
public interface DoublePipelineInitialiser {
	public void initialise(ReusableDoublePipeline pipeline);
}
