#ifndef STACK_OR_HEAP_BUFFER_H
#define STACK_OR_HEAP_BUFFER_H

#include <cstddef>
#include <vector>

namespace mkh_avro2 {

// Scratch storage that lives on the stack up to `Cap` elements and spills to
// a heap vector beyond that. Replaces the hand-rolled "stack array + heap
// vector + size check + pick a pointer" skeleton that encodearray (element
// buffer, varint scratch), encoderecord (found/present) and encodemap (entry
// scratch) each carried their own copy of (#28), so the stack-vs-heap
// decision is made in one place.
//
// T must be trivially constructible: the inline array is deliberately left
// uninitialized, exactly like the raw arrays this replaces.
//
// Two ways to fill it; use one per instance:
//   - acquire(n): storage for n elements when the size is known up front.
//     Contents are not initialized or preserved across calls. May be called
//     repeatedly (e.g. once per loop iteration); the heap vector is only
//     touched when n > Cap.
//   - push_back(v): append one element at a time when the final size is not
//     known. Spills to the heap on the (Cap+1)th element, copying the
//     first Cap elements across.
template <typename T, size_t Cap>
class StackOrHeapBuffer {
  public:
    T*
    acquire(size_t n) {
        size_ = n;
        if (n <= Cap) {
            return stack_;
        }
        heap_.resize(n);
        return heap_.data();
    }

    void
    push_back(const T& v) {
        if (size_ < Cap) {
            stack_[size_] = v;
        } else {
            if (size_ == Cap) {
                heap_.assign(stack_, stack_ + Cap);
            }
            heap_.push_back(v);
        }
        size_++;
    }

    T*
    data() {
        return size_ <= Cap ? stack_ : heap_.data();
    }

    size_t
    size() const {
        return size_;
    }

  private:
    T stack_[Cap];
    std::vector<T> heap_;
    size_t size_ = 0;
};

} // namespace mkh_avro2

#endif
