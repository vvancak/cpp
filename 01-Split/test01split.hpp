#ifndef TEST_01_SPLIT_
#define TEST_01_SPLIT_

#include<tuple>
#include <sstream>
#include<iostream>

namespace splitter {
    //TODO: Remove helpers
    template<typename T>
    void print(T &value) {
        std::cout << "lval " << value << std::endl;
    }

    template<typename T>
    void print(T &&value) {
        std::cout << "rval " << value << std::endl;
    }


    void print(char value) {
        std::cout << "char " << value << std::endl;
    }


    // Return value of the split function - a wrapper over the standard tuple
    template<typename... FT>
    struct Storage {
    public:
        std::tuple<FT...> internal_tuple;

        // public:
        Storage(std::tuple<FT...> &&tpl) : internal_tuple(tpl) {}

        // Copy
        Storage(const Storage &x) = delete;

        // Copy assign
        Storage &operator=(const Storage &x) = delete;

        // Move
        Storage(Storage &&other) : internal_tuple(std::move(other.internal_tuple)) {}

        // Move assign
        Storage &operator=(Storage &&x) noexcept {
            internal_tuple = std::move(x.internal_tuple);
            return *this;
        }
    };

    // API
    template<typename... FT>
    Storage<FT...> split(FT &&... params) {
        std::tuple<FT...> tpl(params...);
        Storage storage(std::move(tpl));
        return storage;
    }

    template<typename... FT>
    std::istream &operator>>(std::istream &in, const Storage<FT...> &storage) {
//        print(std::get<0>(storage.internal_tuple));
//        print(std::get<1>(storage.internal_tuple));
//        print(std::get<2>(storage.internal_tuple));
//        print(std::get<3>(storage.internal_tuple));
//        print(std::get<4>(storage.internal_tuple));
//        print(std::get<5>(storage.internal_tuple));
    }
}

#endif