package Streaming.ReusableStream;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ReusableReferencePipeline<T> extends ReusableAbstractPipeline<T> implements ReusableStream<T> {
	
	public ReusableReferencePipeline(boolean parallel){
		super(parallel);
	}
	
	public ReusableReferencePipeline(ReusableAbstractPipeline<T> upPipeline) {
		super(upPipeline);
	}
	
	@Override
	public Stream<T> filter(Predicate<? super T> predicate) {
		recordCurrentOperation(OperationType.filter, predicate);
		return new ReusableReferencePipeline<T>(this);
	}

	@Override
	public <R> Stream<R> map(Function<? super T, ? extends R> mapper) {
		recordCurrentOperation(OperationType.map, mapper);
		return new ReusableReferencePipeline<R>((ReusableAbstractPipeline<R>) this);
	}

	@Override
	public IntStream mapToInt(ToIntFunction<? super T> mapper) {
		recordCurrentOperation(OperationType.mapToInt, mapper);
		return new ReusableIntPipeline( (ReusableAbstractPipeline<Integer>) this);
	}

	@Override
	public LongStream mapToLong(ToLongFunction<? super T> mapper) {
		recordCurrentOperation(OperationType.mapToLong, mapper);
		return new ReusableLongPipeline( (ReusableAbstractPipeline) this);
	}

	@Override
	public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
		recordCurrentOperation(OperationType.mapToDouble, mapper);
		return new ReusableDoublePipeline( (ReusableAbstractPipeline) this);
	}

	@Override
	public <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
		recordCurrentOperation(OperationType.flatMap, mapper);
		return new ReusableReferencePipeline<R>((ReusableReferencePipeline<R>) this);
	}

	@Override
	public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
		recordCurrentOperation(OperationType.flatMapToInt, mapper);
		return new ReusableIntPipeline( (ReusableAbstractPipeline<Integer>) this);
	}

	@Override
	public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
		recordCurrentOperation(OperationType.flatMapToLong, mapper);
		return new ReusableLongPipeline( (ReusableAbstractPipeline) this);
	}

	@Override
	public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
		recordCurrentOperation(OperationType.flatMapToDouble, mapper);
		return new ReusableDoublePipeline( (ReusableAbstractPipeline) this);
	}

	@Override
	public Stream<T> distinct() {
		recordCurrentOperation(OperationType.distinct);
		return new ReusableReferencePipeline(this);
	}

	@Override
	public Stream<T> sorted() {
		recordCurrentOperation(OperationType.sorted);
		return new ReusableReferencePipeline(this);
	}

	@Override
	public Stream<T> sorted(Comparator<? super T> comparator) {
		recordCurrentOperation(OperationType.sorted_with_argument, comparator);
		return new ReusableReferencePipeline(this);
	}

	@Override
	public Stream<T> peek(Consumer<? super T> action) {
		recordCurrentOperation(OperationType.peek, action);
		return new ReusableReferencePipeline(this);
	}

	@Override
	public Stream<T> limit(long maxSize) {
		recordCurrentOperation(OperationType.limit, maxSize);
		return new ReusableReferencePipeline(this);
	}

	@Override
	public Stream<T> skip(long n) {
		recordCurrentOperation(OperationType.skip, n);
		return new ReusableReferencePipeline(this);
	}

	@Override
	public void forEach(Consumer<? super T> action) {
		recordCurrentOperation(OperationType.forEach, action);
		onTerminalOpsInvoked();
	}

	@Override
	public void forEachOrdered(Consumer<? super T> action) {
		recordCurrentOperation(OperationType.forEachOrdered, action);
		onTerminalOpsInvoked();
	}

	@Override
	public Object[] toArray() {
		recordCurrentOperation(OperationType.toArray);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public <A> A[] toArray(IntFunction<A[]> generator) {
		recordCurrentOperation(OperationType.toArray_with_argument, generator);
		onTerminalOpsInvoked();		
		return null;
	}

	@Override
	public T reduce(T identity, BinaryOperator<T> accumulator) {
		recordCurrentOperation(OperationType.reduce_with_2_arguments, identity, accumulator);
		onTerminalOpsInvoked();	
		return null;
	}

	@Override
	public Optional<T> reduce(BinaryOperator<T> accumulator) {
		recordCurrentOperation(OperationType.reduce_with_1_argument, accumulator);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
		recordCurrentOperation(OperationType.reduce_with_3_arguments, identity, accumulator, combiner);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
		recordCurrentOperation(OperationType.collect, supplier, accumulator, combiner);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public <R, A> R collect(Collector<? super T, A, R> collector) {
		recordCurrentOperation(OperationType.collect_with_collector, collector);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public Optional<T> min(Comparator<? super T> comparator) {
		recordCurrentOperation(OperationType.min_with_1_argument, comparator);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public Optional<T> max(Comparator<? super T> comparator) {
		recordCurrentOperation(OperationType.max_with_1_argument, comparator);
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
	public boolean anyMatch(Predicate<? super T> predicate) {
		recordCurrentOperation(OperationType.anyMatch, predicate);
		onTerminalOpsInvoked();
		return false;
	}

	@Override
	public boolean allMatch(Predicate<? super T> predicate) {
		recordCurrentOperation(OperationType.allMatch, predicate);
		onTerminalOpsInvoked();
		return false;
	}

	@Override
	public boolean noneMatch(Predicate<? super T> predicate) {
		recordCurrentOperation(OperationType.noneMatch, predicate);
		onTerminalOpsInvoked();
		return false;
	}

	@Override
	public Optional<T> findFirst() {
		recordCurrentOperation(OperationType.findFirst);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public Optional<T> findAny() {
		recordCurrentOperation(OperationType.findAny);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public Iterator<T> iterator() {
		recordCurrentOperation(OperationType.iterator);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public Spliterator<T> spliterator() {
		recordCurrentOperation(OperationType.spliterator);
		onTerminalOpsInvoked();
		return null;
	}

	@Override
	public Stream<T> sequential() {
		recordCurrentOperation(OperationType.sequential);
		return new ReusableReferencePipeline(this);
	}

	@Override
	public Stream<T> parallel() {
		recordCurrentOperation(OperationType.parallel);
		return new ReusableReferencePipeline(this);
	}

	@Override
	public Stream<T> unordered() {
		recordCurrentOperation(OperationType.unordered);
		return new ReusableReferencePipeline(this);
	}

	@Override
	public Stream<T> onClose(Runnable closeHandler) {
		recordCurrentOperation(OperationType.onClose, closeHandler);
		return new ReusableReferencePipeline(this);
	}
}
