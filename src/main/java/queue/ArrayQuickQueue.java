package queue;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.Queue;

/* A single-publisher-single-subscriber queue. 
 * 
 * At each moment, there could be only one publisher and one subscriber. But the 
 * publisher and the subscriber can overlap with each other, and that is where 
 * concurrency occurs. 
 * 
 * No lock or CAS is used.
 * 
 * Two indexes, putIndex and takeIndex are used to track the positions of publisher 
 * and subscriber. Both indexes are defined as "volatile int", which is the key to 
 * implement the concurrency without using lock or CAS.
 * 
 * If putIndex == takeIndex, the queue is empty. If putIndex + 1 == takeIndex, the 
 * queue is full. Either index reaches the end of the queue, the index is wrapped 
 * around to the beginning of the queue. 
 * 
 * The size of the queue can be optimized at the order of 2 to improve the speed of 
 * index increment.
 * 
 * peek() and iterator() are not supported at this point.
 * 
 * Testing shows java.util.concurrent.ConcurrentLinkedQueue beats ArrayQuickQueue by 10%.
 * The primary reason is that java.util.concurrent.ConcurrentLinkedQueue is unbound, and 
 * thus adding items always succeeds instantly, while ArrayQuickQueue is bound by array 
 * size, in which case adding items may require multiple retries before it succeeds. 
 * When array size grows bigger, the performance of ArrayQuickQueue converges to that 
 * of java.util.concurrent.ConcurrentLinkedQueue.
 * 
 * 
 * */

public class ArrayQuickQueue<E> extends AbstractQueue<E> implements Queue<E> {

	/* number of items queued */
	private final int size;

	/* queued items */
	private final E[] items;

	/* item index for next poll and remove */
	private volatile int takeIndex;

	/* item index for next offer and add */
	private volatile int putIndex;

	/* index increment */
	private int inc(int pos) {
		return (++pos == size) ? 0 : pos;
	}

	public ArrayQuickQueue(int size) {
		this.size = size;
		this.items = (E[]) (new Object[size]);
		this.takeIndex = 0;
		this.putIndex = 0;
	}

	@Override
	public boolean offer(E e) {
		
		if(e == null) {
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
	public E peek() {
		throw new UnsupportedOperationException();
	}

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

	@Override
	public Iterator<E> iterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int size() {
		return (putIndex + size - takeIndex) % size;
	}

}