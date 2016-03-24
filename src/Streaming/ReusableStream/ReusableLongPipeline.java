package Streaming.ReusableStream;

import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator.OfLong;
import java.util.function.BiConsumer;
import java.util.function.LongBinaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.function.LongToDoubleFunction;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ReusableLongPipeline extends ReusableAbstractPipeline<Long> implements ReusableLongStream {

	public ReusableLongPipeline(boolean parallel) {
		super(parallel);
	}
	public ReusableLongPipeline(ReusableAbstractPipeline upPipeline) {
		super(upPipeline);
	}

	@Override
	public LongStream filter(LongPredicate predicate) {
		recordCurrentOperation(OperationType.filter, predicate);
		return new ReusableLongPipeline(this);
	}

	@Override
	public LongStream map(LongUnaryOperator mapper) {
		recordCurrentOperation(OperationType.map, mapper);
		return new ReusableLongPipeline(this);
	}

	@Override
	public <U> Stream<U> mapToObj(LongFunction<? extends U> mapper) {
		recordCurrentOperation(OperationType.mapToObj, mapper);
		return new ReusableReferencePipeline<U>((ReusableAbstractPipeline) this);
	}

	@Override
	public IntStream mapToInt(LongToIntFunction mapper) {
		recordCurrentOperation(OperationType.mapToInt, mapper);
		return new ReusableIntPipeline( (ReusableAbstractPipeline) this);
	}

	@Override
	public DoubleStream mapToDouble(LongToDoubleFunction mapper) {
		recordCurrentOperation(OperationType.mapToDouble, mapper);
		return new ReusableDoublePipeline( (ReusableAbstractPipeline) this);
	}

	@Override
	public LongStream flatMap(LongFunction<? extends LongStream> mapper) {
		recordCurrentOperation(OperationType.flatMap, mapper);
		return new ReusableLongPipeline(this);
	}

	@Override
	public LongStream distinct() {
		recordCurrentOperation(OperationType.distinct);
		return new ReusableLongPipeline(this);
	}

	@Override
	public LongStream sorted() {
		recordCurrentOperation(OperationType.sorted);
		return new ReusableLongPipeline(this);
	}

	@Override
	public LongStream peek(LongConsumer action) {
		recordCurrentOperation(OperationType.peek, action);
		return new ReusableLongPipeline(this);
	}

	@Override
	public LongStream limit(long maxSize) {
		recordCurrentOperation(OperationType.limit, maxSize);
		return new ReusableLongPipeline(this);
	}

	@Override
	public LongStream skip(long n) {
		recordCurrentOperation(OperationType.skip, n);
		return new ReusableLongPipeline(this);
	}

	@Override
	public void forEach(LongConsumer action) {
		recordCurrentOperation(OperationType.forEach, action);
		onTerminalOpsInvoked();
	}

	@Override
	public void forEachOrdered(LongConsumer action) {
		recordCurrentOperation(OperationType.forEachOrdered, action);
		onTerminalOpsInvoked();
	}

	@Override
	public long[] toArray() {
		recordCurrentOperation(OperationType.toArray);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public long reduce(long identity, LongBinaryOperator op) {
		recordCurrentOperation(OperationType.reduce_with_2_arguments, identity, op);
		onTerminalOpsInvoked();
		return 0;
	}

	@Override
	public OptionalLong reduce(LongBinaryOperator op) {
		recordCurrentOperation(OperationType.reduce_with_1_argument, op);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public <R> R collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner) {
		recordCurrentOperation(OperationType.collect, supplier, accumulator, combiner);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public long sum() {
		recordCurrentOperation(OperationType.sum);
		onTerminalOpsInvoked();
		return 0;
	}

	@Override
	public OptionalLong min() {
		recordCurrentOperation(OperationType.min);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public OptionalLong max() {
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
	public LongSummaryStatistics summaryStatistics() {
		recordCurrentOperation(OperationType.summaryStatistics);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public boolean anyMatch(LongPredicate predicate) {
		recordCurrentOperation(OperationType.anyMatch, predicate);
		onTerminalOpsInvoked();
		return false;
	}

	@Override
	public boolean allMatch(LongPredicate predicate) {
		recordCurrentOperation(OperationType.allMatch, predicate);
		onTerminalOpsInvoked();
		return false;
	}

	@Override
	public boolean noneMatch(LongPredicate predicate) {
		recordCurrentOperation(OperationType.noneMatch, predicate);
		onTerminalOpsInvoked();
		return false;
	}

	@Override
	public OptionalLong findFirst() {
		recordCurrentOperation(OperationType.findFirst);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public OptionalLong findAny() {
		recordCurrentOperation(OperationType.findAny);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public DoubleStream asDoubleStream() {
		recordCurrentOperation(OperationType.asDoubleStream);
		return new ReusableDoublePipeline(this);
	}

	@Override
	public Stream<Long> boxed() {
		recordCurrentOperation(OperationType.boxed);
		return new ReusableReferencePipeline(this);
	}

	@Override
	public LongStream sequential() {
		recordCurrentOperation(OperationType.sequential);
		return new ReusableLongPipeline(this);
	}

	@Override
	public LongStream parallel() {
		recordCurrentOperation(OperationType.parallel);
		return new ReusableLongPipeline(this);
	}

	@Override
	public OfLong iterator() {
		recordCurrentOperation(OperationType.iterator);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public java.util.Spliterator.OfLong spliterator() {
		recordCurrentOperation(OperationType.spliterator);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public LongStream unordered() {
		recordCurrentOperation(OperationType.unordered);
		return new ReusableLongPipeline(this);
	}

	@Override
	public LongStream onClose(Runnable closeHandler) {
		recordCurrentOperation(OperationType.onClose, closeHandler);
		return new ReusableLongPipeline(this);
	}
}
