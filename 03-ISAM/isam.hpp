#ifndef ISAM_ISAM_HPP
#define ISAM_ISAM_HPP

#include "block_provider.hpp"

template<typename TKey, typename TValue>
class isam {
public:
    struct pseudo_pair {
        TKey first;
        TValue second;
    };

    class isam_iter {

        iterator &operator++() {}

        const iterator operator++(int) {}

        pseudo_pair &operator*() {}

        pseudo_pair *operator->() {}

        bool operator==(const iterator &other) const {}

        bool operator!=(const iterator &other) const {}
    };

    isam(size_t block_size, size_t overflow_size) {}

    TValue &operator[](TKey key) {}

    isam_iter begin() {
        return isam_iter();
    }

    isam_iter end() {
        return isam_iter();
    }

private:
};

#endif // ISAM_ISAM_HPP
