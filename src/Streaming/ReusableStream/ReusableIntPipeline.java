package Streaming.ReusableStream;

import java.util.IntSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator.OfInt;
import java.util.function.BiConsumer;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ReusableIntPipeline extends ReusableAbstractPipeline<Integer> implements ReusableIntStream{
	
	public ReusableIntPipeline(boolean parallel) {
		super(parallel);
	}

	public ReusableIntPipeline(ReusableAbstractPipeline upPipeline) {
		super(upPipeline);
	}

	@Override
	public IntStream filter(IntPredicate predicate) {
		recordCurrentOperation(OperationType.filter, predicate);
		return new ReusableIntPipeline(this);
	}

	@Override
	public IntStream map(IntUnaryOperator mapper) {
		recordCurrentOperation(OperationType.map, mapper);
		return new ReusableIntPipeline(this);
	}

	@Override
	public <U> Stream<U> mapToObj(IntFunction<? extends U> mapper) {
		recordCurrentOperation(OperationType.mapToObj, mapper);
		return new ReusableReferencePipeline<U>((ReusableAbstractPipeline) this);
	}

	@Override
	public LongStream mapToLong(IntToLongFunction mapper) {
		recordCurrentOperation(OperationType.mapToLong, mapper);
		return new ReusableLongPipeline( (ReusableAbstractPipeline) this);
	}

	@Override
	public DoubleStream mapToDouble(IntToDoubleFunction mapper) {
		recordCurrentOperation(OperationType.mapToDouble, mapper);
		return new ReusableDoublePipeline( (ReusableAbstractPipeline) this);
	}

	@Override
	public IntStream flatMap(IntFunction<? extends IntStream> mapper) {
		recordCurrentOperation(OperationType.flatMap, mapper);
		return new ReusableIntPipeline(this);
	}

	@Override
	public IntStream distinct() {
		recordCurrentOperation(OperationType.distinct);
		return new ReusableIntPipeline(this); 
	}

	@Override
	public IntStream sorted() {
		recordCurrentOperation(OperationType.sorted);
		return new ReusableIntPipeline(this);
	}

	@Override
	public IntStream peek(IntConsumer action) {
		recordCurrentOperation(OperationType.peek, action);
		return new ReusableIntPipeline(this);
	}

	@Override
	public IntStream limit(long maxSize) {
		recordCurrentOperation(OperationType.limit, maxSize);
		return new ReusableIntPipeline(this);
	}

	@Override
	public IntStream skip(long n) {
		recordCurrentOperation(OperationType.skip, n);
		return new ReusableIntPipeline(this);
	}

	@Override
	public void forEach(IntConsumer action) {
		recordCurrentOperation(OperationType.forEach, action);
		onTerminalOpsInvoked();
	}

	@Override
	public void forEachOrdered(IntConsumer action) {
		recordCurrentOperation(OperationType.forEachOrdered, action);
		onTerminalOpsInvoked();
	}

	@Override
	public int[] toArray() {
		recordCurrentOperation(OperationType.toArray);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public int reduce(int identity, IntBinaryOperator op) {
		recordCurrentOperation(OperationType.reduce_with_2_arguments,identity,op);
		onTerminalOpsInvoked();
		return 0;
	}

	@Override
	public OptionalInt reduce(IntBinaryOperator op) {
		recordCurrentOperation(OperationType.reduce_with_1_argument,op);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public <R> R collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner) {
		recordCurrentOperation(OperationType.collect, supplier, accumulator, combiner);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public int sum() {
		recordCurrentOperation(OperationType.sum);
		onTerminalOpsInvoked();
		return 0;
	}

	@Override
	public OptionalInt min() {
		recordCurrentOperation(OperationType.min);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public OptionalInt max() {
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
	public IntSummaryStatistics summaryStatistics() {
		recordCurrentOperation(OperationType.summaryStatistics);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public boolean anyMatch(IntPredicate predicate) {
		recordCurrentOperation(OperationType.anyMatch, predicate);
		onTerminalOpsInvoked();
		return false;
	}

	@Override
	public boolean allMatch(IntPredicate predicate) {
		recordCurrentOperation(OperationType.allMatch, predicate);
		onTerminalOpsInvoked();
		return false;
	}

	@Override
	public boolean noneMatch(IntPredicate predicate) {
		recordCurrentOperation(OperationType.noneMatch, predicate);
		onTerminalOpsInvoked();
		return false;
	}

	@Override
	public OptionalInt findFirst() {
		recordCurrentOperation(OperationType.findFirst);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public OptionalInt findAny() {
		recordCurrentOperation(OperationType.findAny);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public LongStream asLongStream() {
		recordCurrentOperation(OperationType.asLongStream);
		return new ReusableLongPipeline(this);
	}

	@Override
	public DoubleStream asDoubleStream() {
		recordCurrentOperation(OperationType.asDoubleStream);
		return new ReusableDoublePipeline(this);
	}

	@Override
	public Stream<Integer> boxed() {
		recordCurrentOperation(OperationType.boxed);
		return new ReusableReferencePipeline(this);
	}

	@Override
	public IntStream sequential() {
		recordCurrentOperation(OperationType.sequential);
		return new ReusableIntPipeline(this);
	}

	@Override
	public IntStream parallel() {
		recordCurrentOperation(OperationType.parallel);
		return new ReusableIntPipeline(this);
	}

	@Override
	public OfInt iterator() {
		recordCurrentOperation(OperationType.iterator);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public java.util.Spliterator.OfInt spliterator() {
		recordCurrentOperation(OperationType.spliterator);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public IntStream unordered() {
		recordCurrentOperation(OperationType.unordered);
		return new ReusableIntPipeline(this);
	}

	@Override
	public IntStream onClose(Runnable closeHandler) {
		recordCurrentOperation(OperationType.onClose, closeHandler);
		return new ReusableIntPipeline(this);
	}
}
