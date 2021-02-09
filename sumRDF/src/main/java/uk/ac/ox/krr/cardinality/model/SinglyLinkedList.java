package uk.ac.ox.krr.cardinality.model;

public abstract class SinglyLinkedList<E> {

	protected E head;
	protected E tail;

	public abstract E getNext(E element);

	protected abstract void setNext(E element, E next);

	public SinglyLinkedList() {
	}

	public void clear() {
		head = null;
		tail = null;
	}

	public boolean isEmpty() {
		return head == null;
	}

	public E getHead() {
		return head;
	}

	public E getTail() {
		return tail;
	}

	/**
	 * Inserts the given element at the start of the list.
	 * 
	 * @param element
	 */
	public void insertFirst(E element) {
		assert element != null;
		if (head == null) {
			tail = element;
		}
		setNext(element, head);
		head = element;
	}

	public void setTail(E element) {
		this.tail = element;
	}

	/**
	 * Insert the given element at the end of the list
	 * 
	 * @param element
	 */
	public void append(E element) {
		assert element != null;
		setNext(element, null);
		if (tail != null) {
			setNext(tail, element);
		} else {
			assert head == null;
			head = element;
		}
		tail = element;
	}

	public void append(SinglyLinkedList<E> list) {
		if (!list.isEmpty()) {
			if (tail != null)
				setNext(tail, list.head);
			else
				head = list.head;
			tail = list.tail;
		}
	}

	/**
	 * Removes the given element from the list
	 * 
	 * @param element
	 */
	public void delete(E element) {
		assert element != null;
		E current = head;
		E previous = null;
		while (current != null) {
			if (current == element)
				break;
			previous = current;
			current = getNext(current);
		}
		if (current != null) {
			if (previous == null) {
				head = getNext(current);
				setNext(current, null);
			} else {
				setNext(previous, getNext(current));
				setNext(current, null);
				if (getNext(previous) == null) {
					tail = previous;
				}
			}
		}
	}

	public boolean contains(E element) {
		E current = head;
		while (current != null) {
			if (current == element)
				return true;
			current = getNext(current);
		}
		return false;
	}

	public int size() {
		if (head == null)
			return 0;
		E element = head;
		int counter = 0;
		while (element != null) {
			counter++;
			element = getNext(element);
		}
		return counter;
	}
}
