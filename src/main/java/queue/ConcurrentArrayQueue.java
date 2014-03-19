package queue;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ConcurrentArrayQueue<E> extends AbstractQueue<E> implements Queue<E> {

	private final int size;

	private final AtomicReference<E>[] items;

	private final AtomicInteger putIndex;

	private final AtomicInteger takeIndex;

	private int inc(int pos) {
		return (++pos == size) ? 0 : pos;
	}

	public ConcurrentArrayQueue(int size) {
		this.items = new AtomicReference[size];
		for (int i = 0; i < size; ++i) {
			items[i] = new AtomicReference<E>();
		}
		this.size = size;
		this.putIndex = new AtomicInteger(0);
		this.takeIndex = new AtomicInteger(0);
	}

	@Override
	public boolean offer(E e) {

		if (e == null) {
			throw new NullPointerException();
		}

		while (true) {

			int oldPutIndex = putIndex.get();
			int newPutIndex = inc(oldPutIndex);

			if (newPutIndex == this.takeIndex.get()) {
				return false;
			}

			if (items[oldPutIndex].compareAndSet(null, e)) {
				putIndex.compareAndSet(oldPutIndex, newPutIndex);
				return true;
			} else {
				putIndex.compareAndSet(oldPutIndex, newPutIndex);
			}
		}
	}

	@Override
	public E poll() {

		while (true) {

			int oldTakeIndex = takeIndex.get();
			int newTakeIndex = inc(oldTakeIndex);

			if (oldTakeIndex == this.putIndex.get()) {
				return null;
			}

			E e = items[oldTakeIndex].get();
			if (e != null && items[oldTakeIndex].compareAndSet(e, null)) {

				takeIndex.compareAndSet(oldTakeIndex, newTakeIndex);
				return e;
			} else {
				takeIndex.compareAndSet(oldTakeIndex, newTakeIndex);
			}
		}
	}

	@Override
	public E peek() {
		while (true) {

			int oldTakeIndex = takeIndex.get();
			int newTakeIndex = inc(oldTakeIndex);

			if (oldTakeIndex == this.putIndex.get()) {
				return null;
			}

			E e = items[oldTakeIndex].get();
			if (e != null) {
				return e;
			} else {
				takeIndex.compareAndSet(oldTakeIndex, newTakeIndex);
			}
		}
	}

	@Override
	public boolean isEmpty() {
		return putIndex.get() == takeIndex.get();
	}

	@Override
	public int size() {
		return (putIndex.get() + size - takeIndex.get()) % size;
	}

	@Override
	public Iterator<E> iterator() {
		return new Iter();
	}

	private class Iter implements Iterator<E> {

		private int curPos;
		private E curObject;

		Iter() {
			init();
		}

		private void init() {

			curPos = takeIndex.get();

			while (true) {

				if (curPos == putIndex.get()) {
					curObject = null;
					break;
				}

				curObject = items[curPos].get();
				if (curObject != null) {
					break;
				}

				curPos = takeIndex.get();
			}
		}

		@Override
		public boolean hasNext() {
			return !(curObject == null);
		}

		@Override
		public E next() {

			if (curObject == null) {
				throw new NoSuchElementException();
			}

			E obj = curObject;

			while (true) {

				curPos = inc(curPos);

				if (curPos == putIndex.get()) {
					curObject = null;
					break;
				}

				curObject = items[curPos].get();
				if (curObject != null) {
					break;
				}
			}

			return obj;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
