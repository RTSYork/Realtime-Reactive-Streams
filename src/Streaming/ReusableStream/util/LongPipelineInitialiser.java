package Streaming.ReusableStream.util;

import Streaming.ReusableStream.ReusableLongPipeline;

@FunctionalInterface
public interface LongPipelineInitialiser {
	public void initialise(ReusableLongPipeline pipeline);
}
