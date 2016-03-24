package Streaming.ReusableStream;

import java.util.DoubleSummaryStatistics;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator.OfDouble;
import java.util.function.BiConsumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleToIntFunction;
import java.util.function.DoubleToLongFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.ObjDoubleConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ReusableDoublePipeline extends ReusableAbstractPipeline<Double> implements ReusableDoubleStream {
	
	public ReusableDoublePipeline(boolean parallel) {
		super(parallel);
	}

	public ReusableDoublePipeline(ReusableAbstractPipeline upPipeline) {
		super(upPipeline);
	}

	@Override
	public DoubleStream filter(DoublePredicate predicate) {
		recordCurrentOperation(OperationType.filter, predicate);
		return new ReusableDoublePipeline(this);
	}

	@Override
	public DoubleStream map(DoubleUnaryOperator mapper) {
		recordCurrentOperation(OperationType.map, mapper);
		return new ReusableDoublePipeline(this);
	}

	@Override
	public <U> Stream<U> mapToObj(DoubleFunction<? extends U> mapper) {
		recordCurrentOperation(OperationType.mapToObj, mapper);
		return new ReusableReferencePipeline<U>((ReusableAbstractPipeline) this);
	}

	@Override
	public IntStream mapToInt(DoubleToIntFunction mapper) {
		recordCurrentOperation(OperationType.mapToInt, mapper);
		return new ReusableIntPipeline( (ReusableAbstractPipeline) this);
	}

	@Override
	public LongStream mapToLong(DoubleToLongFunction mapper) {
		recordCurrentOperation(OperationType.mapToLong, mapper);
		return new ReusableLongPipeline( (ReusableAbstractPipeline) this);
	}

	@Override
	public DoubleStream flatMap(DoubleFunction<? extends DoubleStream> mapper) {
		recordCurrentOperation(OperationType.flatMap, mapper);
		return new ReusableDoublePipeline(this);
	}

	@Override
	public DoubleStream distinct() {
		recordCurrentOperation(OperationType.distinct);
		return new ReusableDoublePipeline(this);
	}

	@Override
	public DoubleStream sorted() {
		recordCurrentOperation(OperationType.sorted);
		return new ReusableDoublePipeline(this);
	}

	@Override
	public DoubleStream peek(DoubleConsumer action) {
		recordCurrentOperation(OperationType.peek, action);
		return new ReusableDoublePipeline(this);
	}

	@Override
	public DoubleStream limit(long maxSize) {
		recordCurrentOperation(OperationType.limit, maxSize);
		return new ReusableDoublePipeline(this);
	}

	@Override
	public DoubleStream skip(long n) {
		recordCurrentOperation(OperationType.skip, n);
		return new ReusableDoublePipeline(this);
	}

	@Override
	public void forEach(DoubleConsumer action) {
		recordCurrentOperation(OperationType.forEach, action);
		onTerminalOpsInvoked();
	}

	@Override
	public void forEachOrdered(DoubleConsumer action) {
		recordCurrentOperation(OperationType.forEachOrdered, action);
		onTerminalOpsInvoked();
	}

	@Override
	public double[] toArray() {
		recordCurrentOperation(OperationType.toArray);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public double reduce(double identity, DoubleBinaryOperator op) {
		recordCurrentOperation(OperationType.reduce_with_2_arguments, identity, op);
		onTerminalOpsInvoked();
		return 0;
	}

	@Override
	public OptionalDouble reduce(DoubleBinaryOperator op) {
		recordCurrentOperation(OperationType.reduce_with_1_argument, op);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public <R> R collect(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner) {
		recordCurrentOperation(OperationType.collect, supplier, accumulator, combiner);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public double sum() {
		recordCurrentOperation(OperationType.sum);
		onTerminalOpsInvoked();
		return 0;
	}

	@Override
	public OptionalDouble min() {
		recordCurrentOperation(OperationType.min);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public OptionalDouble max() {
		recordCurrentOperation(OperationType.max);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public long count() {
		recordCurrentOperation(OperationType.count);
		onTerminalOpsInvoked();
		return 0;
	}

	@Override
	public OptionalDouble average() {
		recordCurrentOperation(OperationType.average);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public DoubleSummaryStatistics summaryStatistics() {
		recordCurrentOperation(OperationType.summaryStatistics);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public boolean anyMatch(DoublePredicate predicate) {
		recordCurrentOperation(OperationType.anyMatch, predicate);
		onTerminalOpsInvoked();
		return false;
	}

	@Override
	public boolean allMatch(DoublePredicate predicate) {
		recordCurrentOperation(OperationType.allMatch, predicate);
		onTerminalOpsInvoked();
		return false;
	}

	@Override
	public boolean noneMatch(DoublePredicate predicate) {
		recordCurrentOperation(OperationType.noneMatch, predicate);
		onTerminalOpsInvoked();
		return false;
	}

	@Override
	public OptionalDouble findFirst() {
		recordCurrentOperation(OperationType.findFirst);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public OptionalDouble findAny() {
		recordCurrentOperation(OperationType.findAny);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public Stream<Double> boxed() {
		recordCurrentOperation(OperationType.boxed);
		return new ReusableReferencePipeline(this);
	}

	@Override
	public DoubleStream sequential() {
		recordCurrentOperation(OperationType.sequential);
		return new ReusableDoublePipeline(this);
	}

	@Override
	public DoubleStream parallel() {
		recordCurrentOperation(OperationType.parallel);
		return new ReusableDoublePipeline(this);
	}

	@Override
	public OfDouble iterator() {
		recordCurrentOperation(OperationType.iterator);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public java.util.Spliterator.OfDouble spliterator() {
		recordCurrentOperation(OperationType.spliterator);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public DoubleStream unordered() {
		recordCurrentOperation(OperationType.unordered);
		return new ReusableDoublePipeline(this);
	}

	@Override
	public DoubleStream onClose(Runnable closeHandler) {
		recordCurrentOperation(OperationType.onClose, closeHandler);
		return new ReusableDoublePipeline(this);
	}
}
