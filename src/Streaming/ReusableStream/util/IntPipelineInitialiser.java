package Streaming.ReusableStream.util;

import Streaming.ReusableStream.ReusableIntPipeline;

@FunctionalInterface
public interface IntPipelineInitialiser {
	public void initialise(ReusableIntPipeline pipeline);
}
