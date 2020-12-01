// container.h - support for standard containers.

#pragma once

extern "C" {
#include <runtime/preempt.h>
}

#include <limits>
#include <vector>
#include <list>
#include <deque>
#include <unordered_map>

namespace rt {

// Allocator support
template <typename T>
struct allocator {
  using value_type = T;

  allocator() = default;
  template <class U>
  allocator(const allocator<U>&) {}

  T* allocate(std::size_t n) {
    if (n <= std::numeric_limits<std::size_t>::max() / sizeof(T)) {
      preempt_disable();
      void* ptr = std::malloc(n * sizeof(T));
      preempt_enable();
      if (ptr)
        return static_cast<T*>(ptr);
    }
    throw std::bad_alloc();
  }

  void deallocate(T* ptr, std::size_t n) {
    preempt_disable();
    std::free(ptr);
    preempt_enable();
  }
};

template <typename T, typename U>
inline bool operator == (const allocator<T>&, const allocator<U>&) {
  return true;
}

template <typename T, typename U>
inline bool operator != (const allocator<T>& a, const allocator<U>& b) {
  return !(a == b);
}

using string = std::basic_string<char, std::char_traits<char>, allocator<char>>;

template<typename T>
using vector = std::vector<T, allocator<T>>;

template<typename T>
using list = std::list<T, allocator<T>>;

template<typename T>
using deque = std::list<T, allocator<T>>;

template<typename K, typename V>
using unordered_map = std::unordered_map<K, V, allocator<std::pair<K, V>>>;

} // namespace rt
