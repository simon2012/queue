package queue;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/* A single-publisher-single-subscriber blocking queue 
 * 
 * ArrayQuickBlockingQueue is an extension of ArrayQuickQueue. It not only has 
 * the same performance of adding and removing item that ArrayQuickQueue does, 
 * but also supports the blocking mechanism that java.util.concurrent.BlockingQueue 
 * defines. 
 * 
 * When an item is added into queue, if queue is not full, the item will be added 
 * instantly with no use of lock or CAS. However, if queue is full, the caller will 
 * be blocked and later notified when the queue is not full. When an item is removed 
 * from queue, if the queue is not empty, the item will be removed instantly with
 * no use of lock or CAS. However, if queue is empty, the caller will be blocked and 
 * later notified when the queue is not empty. 
 * 
 * peek(), iterator(), and drainTo() are not supported at this point.
 * 
 * Testing shows that: 
 * For array size less than 200, java.util.concurrent.ArrayBlockingQueue beats ArrayQuickBlockingQueue.
 * For array size larger than 200, ArrayQuickBlockingQueue beats java.util.concurrent.ArrayBlockingQueue.
 * 
 * Testing is performed against on a single-publisher-single-subscriber basis.
 * 
 * */

public class ArrayQuickBlockingQueue<E> extends AbstractQueue<E> implements
		BlockingQueue<E> {

	/* number of items queued */
	private final int size;

	/* queued items */
	private final E[] items;

	/* item index for next poll and remove */
	private volatile int takeIndex;

	/* item index for next offer and add */
	private volatile int putIndex;

	/* used for notification */
	private final ReentrantLock lock;

	/* used for notification */
	private final Condition cond;

	/* index increment */
	private int inc(int pos) {
		return (++pos == size) ? 0 : pos;
	}

	public ArrayQuickBlockingQueue(int size) {
		this.size = size;
		this.items = (E[]) new Object[size];
		this.takeIndex = 0;
		this.putIndex = 0;
		this.lock = new ReentrantLock();
		this.cond = lock.newCondition();
	}

	@Override
	public E take() throws InterruptedException {

		final ReentrantLock lock = this.lock;

		int oldTakeIndex = takeIndex;

		/* while loop to check whether the queue is empty or not. */

		while (oldTakeIndex == putIndex) {

			/*
			 * If queue has been found empty, we acquire the lock and check
			 * again. It is possible that queue is empty before we acquire the
			 * lock but not empty after we acquire.
			 */

			lock.lockInterruptibly();
			try {
				if (oldTakeIndex == putIndex) {
					cond.await();
				}

				/*
				 * else, we release the lock and check again. It is possible
				 * that queue is not empty before we release the lock and
				 * becomes empty after we release the lock.
				 */

			} finally {

				lock.unlock();
			}
		}

		E e = items[oldTakeIndex];
		takeIndex = inc(oldTakeIndex);

		/*
		 * If the queue was full, the publisher must be blocked if there is one.
		 */

		if (inc(putIndex) == oldTakeIndex) {
			lock.lock();

			/*
			 * No need to check again, because there could be no more than two
			 * threads, with one being the subscriber ourselves. If queue was
			 * full, the publisher must be blocked if there is one. Thus, the
			 * state of putIndex and takeIndex will not change after we acquire
			 * the lock.
			 */

			try {
				cond.signal();
			} finally {
				lock.unlock();
			}
		}

		return e;
	}

	@Override
	public void put(E e) throws InterruptedException {

		if (e == null) {
			throw new NullPointerException();
		}

		final ReentrantLock lock = this.lock;
		int oldPutIndex = putIndex;
		int newPutIndex = inc(putIndex);

		/* while loop to check whether the queue is full or not. */

		while (newPutIndex == takeIndex) {

			/*
			 * If queue has been found full, we acquire the lock and check
			 * again. It is possible that queue is full before we acquire the
			 * lock but not full after we acquire.
			 */

			lock.lockInterruptibly();
			try {
				if (newPutIndex == takeIndex) {
					cond.await();
				}

				/*
				 * else, we release the lock and check again. It is possible
				 * that queue is not full before we release the lock and becomes
				 * full after we release the lock.
				 */

			} finally {
				lock.unlock();
			}
		}

		items[putIndex] = e;
		putIndex = newPutIndex;

		/*
		 * If the queue was empty, the subscriber must be blocked if there is
		 * one.
		 */

		if (oldPutIndex == takeIndex) {
			lock.lock();
			try {

				/*
				 * No need to check again, because there could be no more than
				 * two threads, with one being the publisher ourselves. If queue
				 * was empty, the subscriber must be blocked if there is one.
				 * Thus, the state of putIndex and takeIndex will not change
				 * after we acquire the lock.
				 */

				cond.signal();
			} finally {
				lock.unlock();
			}
		}
	}

	@Override
	public E peek() {
		throw new UnsupportedOperationException();
	}

	/*
	 * instant remove item if possible; otherwise null is returned. No lock is
	 * involved.
	 */

	@Override
	public E poll() {

		int index = this.putIndex;
		if (index != takeIndex) {

			/* order has to be maintained. */

			E e = items[takeIndex];
			takeIndex = inc(takeIndex);
			return e;
		}
		return null;
	}

	/*
	 * instant add item if possible; otherwise false is returned. No lock is
	 * used.
	 */

	@Override
	public boolean offer(E e) {

		if (e == null) {
			throw new NullPointerException();
		}

		int index = this.takeIndex;
		if (inc(putIndex) != index) {

			/* order has to be maintained. */

			items[putIndex] = e;
			putIndex = inc(putIndex);

			return true;
		}

		return false;
	}

	@Override
	public int drainTo(Collection<? super E> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int drainTo(Collection<? super E> c, int maxElements) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean offer(E e, long timeout, TimeUnit unit)
			throws InterruptedException {

		if (e == null) {
			throw new NullPointerException();
		}

		final ReentrantLock lock = this.lock;
		int oldPutIndex = putIndex;
		int newPutIndex = inc(putIndex);
		long nanos = unit.toNanos(timeout);

		/* while loop to check whether the queue is full or not. */

		while (newPutIndex == takeIndex) {

			/*
			 * If queue has been found full, we acquire the lock and check
			 * again. It is possible that queue is full before we acquire the
			 * lock but not full after we acquire.
			 */

			lock.lockInterruptibly();
			try {
				if (newPutIndex == takeIndex) {

					if (nanos <= 0) {
						return false;
					}

					nanos = cond.awaitNanos(nanos);
				}

				/*
				 * else, we release the lock and check again. It is possible
				 * that queue is not full before we release the lock and becomes
				 * full after we release the lock.
				 */

			} finally {
				lock.unlock();
			}
		}

		items[putIndex] = e;
		putIndex = newPutIndex;

		/*
		 * If the queue was empty, the subscriber must be blocked if there is
		 * one.
		 */

		if (oldPutIndex == takeIndex) {
			lock.lock();
			try {

				/*
				 * No need to check again, because there could be no more than
				 * two threads, with one being the publisher ourselves. If queue
				 * was empty, the subscriber must be blocked if there is one.
				 * Thus, the state of putIndex and takeIndex will not change
				 * after we acquire the lock.
				 */

				cond.signal();
			} finally {
				lock.unlock();
			}
		}

		return true;
	}

	@Override
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {

		long nanos = unit.toNanos(timeout);

		final ReentrantLock lock = this.lock;

		int oldTakeIndex = takeIndex;

		/* while loop to check whether the queue is empty or not. */

		while (oldTakeIndex == putIndex) {

			/*
			 * If queue has been found empty, we acquire the lock and check
			 * again. It is possible that queue is empty before we acquire the
			 * lock but not empty after we acquire.
			 */

			lock.lockInterruptibly();
			try {
				if (oldTakeIndex == putIndex) {

					if (nanos <= 0) {
						return null;
					}

					nanos = cond.awaitNanos(nanos);
				}

				/*
				 * else, we release the lock and check again. It is possible
				 * that queue is not empty before we release the lock and
				 * becomes empty after we release the lock.
				 */

			} finally {

				lock.unlock();
			}
		}

		E e = items[oldTakeIndex];
		takeIndex = inc(oldTakeIndex);

		/*
		 * If the queue was full, the publisher must be blocked if there is one.
		 */

		if (inc(putIndex) == oldTakeIndex) {
			lock.lock();

			/*
			 * No need to check again, because there could be no more than two
			 * threads, with one being the subscriber ourselves. If queue was
			 * full, the publisher must be blocked if there is one. Thus, the
			 * state of putIndex and takeIndex will not change after we acquire
			 * the lock.
			 */

			try {
				cond.signal();
			} finally {
				lock.unlock();
			}
		}

		return e;
	}

	@Override
	public int remainingCapacity() {
		return size - size();
	}

	@Override
	public Iterator<E> iterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int size() {
		return (putIndex + size - takeIndex) % size;
	}

}
