package Streaming.ReusableStream.util;

import Streaming.ReusableStream.ReusableReferencePipeline;

@FunctionalInterface
public interface ReferencePipelineInitialiser<T> {
	public void initialise(ReusableReferencePipeline<T> pipeline);
}
