package collection

import java.util.concurrent.atomic.AtomicInteger

/**
 * Creates a new instance of the array with the given size.
 */
class RingBuffer<T>(bufferSize: Int) : Iterable<T> {

    private val arr: Array<Any?>
    private var _size: Int = 0
    private var tail: Int

    private val head: Int
        get() = if (_size == arr.size) (tail + 1) % _size else 0

    /**
     * Number of elements currently stored in the array.
     */
    val size: Int
        get() = _size

    /**
     * Add an element to the array.
     */
    fun add(item: T) {
        tail = (tail + 1) % arr.size
        arr[tail] = item
        if (_size < arr.size) _size++
    }

    /**
     * Returns true if array is full.
     */
    fun hasEnoughElements(): Boolean =
        _size == this.arr.size

    /**
     * Returns first element of array
     */
    fun firstElement(): T =
        this[head]

    /**
     * Returns last element of array
     */
    fun lastElement(): T =
        this[tail]


    /**
     * Get an element from the array.
     */
    @Suppress("UNCHECKED_CAST")
    operator fun get(index: Int): T =
        when {
            _size == 0 || index > _size || index < 0 -> throw IndexOutOfBoundsException("$index")
            _size == arr.size -> arr[(head + index) % arr.size]
            else -> arr[index]
        } as T

    /**
     * This array as a list.
     */
    @Suppress("UNCHECKED_CAST")
    fun toList(): List<T> = iterator().asSequence().toList()

    override fun iterator(): Iterator<T> = object : Iterator<T> {
        private val index: AtomicInteger = AtomicInteger(0)

        override fun hasNext(): Boolean = index.get() < size

        override fun next(): T = get(index.getAndIncrement())
    }

    init {
        this.arr = arrayOfNulls(bufferSize)
        this.tail = -1
    }

}

fun RingBuffer<Long>.hasIncreasingTrajectory(increasingThreshold: Long): Boolean {
    if (!this.hasEnoughElements()) return false

    val ringBufferList = this.toList()
    if (ringBufferList.size < 2) return true

    for ((current, next) in (ringBufferList.take(ringBufferList.size - 1) zip ringBufferList.takeLast(ringBufferList.size - 1))) {
        if ((next - current) < increasingThreshold) return false
    }

    return true
}

fun RingBuffer<Long>.hasNotChanged(): Boolean {
    if (!this.hasEnoughElements()) return false

    val ringBufferList = this.toList()
    if (ringBufferList.size < 2) return true

    for ((current, next) in (ringBufferList.take(ringBufferList.size - 1) zip ringBufferList.takeLast(ringBufferList.size - 1))) {
        if (next != current) return false
    }

    return true
}
