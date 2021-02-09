package uk.ac.ox.krr.cardinality.model;

public abstract class DoublyLinkedList<E> {

	protected E head;
	protected E tail;

	public abstract E getNext(E element);
	public abstract E getPrevious(E element);

	protected abstract void setNext(E element, E next);

	protected abstract void setPrevious(E element, E previous);
	

	public DoublyLinkedList() {}
	
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
	 * @param element
	 */
	public void insertFirst(E element) {
		if (element != null) {
			if (head != null) {
				setPrevious(head, element);
			} else {
				assert tail == null;
				tail = element;
			}
			setNext(element, head);
			head = element;
			setPrevious(element, null);
		}
	} 
	
	public void setTail(E element) {
		this.tail = element;
	}

	/**
	 * Insert the given element at the end of the list
	 * @param element
	 */
	public void append(E element) {
		if (element != null) {
			setPrevious(element, tail);
			setNext(element, null);
			if (tail != null) {
				setNext(tail, element);
			} else {
				assert head == null;
				head = element;
			}
			tail = element;
		}	
	}
	
	public void append(DoublyLinkedList<E> list) {
		if (!list.isEmpty()) {
			setPrevious(list.head, tail);
			if (tail != null)
				setNext(tail, list.head);
			else 
				head = list.head;
			tail = list.tail;
		}
	}

	/**
	 * Removes the given element from the list
	 * @param element
	 */
	public void delete(E element) {
		if (element != null)  {
			assert contains(element);
			if (getPrevious(element) != null) {
				setNext(getPrevious(element), getNext(element));
			} else {
				head = getNext(element);
			}
			if (getNext(element) != null) {
				setPrevious(getNext(element), getPrevious(element));
			} else {
				tail = getPrevious(element);
			}
			setNext(element, null);
			setPrevious(element, null);
			assert !contains(element);
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
