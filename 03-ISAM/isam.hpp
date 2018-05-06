#ifndef ISAM_ISAM_HPP
#define ISAM_ISAM_HPP

// Vladislav Vancak

#include<map>
#include<list>
#include"block_provider.hpp"

template<typename TKey, typename TValue>
class isam {
public:
    class iterator;

    isam(size_t block_size, size_t overflow_size) :
            block_size(block_size),
            overflow_size(overflow_size),
            file_block_map{},
            overflow{} {}

    TValue &operator[](const TKey &key) {
        key_interval ki(key);
        return get_single_key_interval(ki);
    }

    TValue &operator[](TKey &&key) {
        key_interval ki(key);
        return get_single_key_interval(ki);
    }

    iterator begin();

    iterator end();

private:
    typedef std::pair<TKey, TValue> key_value_pair;

    class file_block {
    public:
        file_block *next = 0;

        file_block() = default;

        explicit file_block(isam *const parent) :
                current_size(0),
                isam_members(parent) {
            block_id = block_provider::create_block(isam_members->block_size * sizeof(key_value_pair));
        }

        void load() {
            values = static_cast<key_value_pair *>(block_provider::load_block(block_id));
        }

        void store() const {
            auto *block_ptr = static_cast<void *>(values);
            block_provider::store_block(block_id, block_ptr);
        }

        void free() const {
            block_provider::free_block(block_id);
        }

        const TKey &get_min_key() const {
            return values[0].first;
        }

        const TKey &get_max_key() const {
            return values[current_size - 1].first;
        }

        const size_t get_max_index() const {
            return current_size;
        }

        const size_t get_block_id() const {
            return block_id;
        }

        int32_t find(const TKey &key) const {
            int32_t upper = current_size - 1;
            int32_t lower = 0;

            while (upper > lower) {
                int32_t index = (upper + lower) / 2;
                TKey current_key = values[index].first;

                if (key < current_key) upper = index - 1;
                else if (current_key < key) lower = index + 1;
                else return index;
            }

            if (!(values[lower].first < key) && !(key < values[lower].first)) return lower;
            else return -1;
        }

        TValue &operator[](size_t index) {
            return values[index].second;
        }

        void merge_overflow(std::map<TKey, TValue> &overflow) {
            const size_t max_size = isam_members->block_size;

            // Create a file_block_map with current values
            std::map<TKey, TValue> block_values;
            for (size_t i = 0; i < current_size; ++i) {
                block_values.emplace(values[i]);
            }

            // Merge with overflow
            while (true) {
                std::cout << "REMOVE: merge" << std::endl;

                // can push more values => emplace values from overflow
                if (current_size <= max_size && overflow.size() > 0) {
                    block_values.emplace(*overflow.begin());
                    overflow.erase(overflow.begin());

                    ++current_size;
                }

                // too much values => move max to overflow
                if (current_size > max_size) {
                    overflow.emplace(*(--block_values.end()));
                    block_values.erase(--block_values.end());
                    --current_size;
                }

                // empty overflow
                if (overflow.size() == 0) break;

                // order check
                if ((--block_values.end())->first < overflow.begin()->first) break;
            }

            // Store values
            size_t idx = 0;
            for (auto &&kvp : block_values) {
                values[idx++] = kvp;
            }
        }

    private:
        isam *isam_members = 0;
        size_t block_id = 0;
        size_t current_size = 0;
        key_value_pair *values{};
    };

    struct key_interval {
        key_interval() = default;

        explicit key_interval(TKey &&key) : min_key(key), max_key(min_key) {}

        explicit key_interval(const TKey &key) : min_key(key), max_key(key) {}

        key_interval(const TKey &min, const TKey &max) : min_key(min), max_key(max) {}

        TKey min_key;
        TKey max_key;
    };

    struct key_interval_comparator {
        bool operator()(const key_interval &first, const key_interval &second) const {
            return first.max_key < second.min_key;
        }
    };

    const size_t block_size;

    const size_t overflow_size;

    std::map<key_interval, file_block, key_interval_comparator> file_block_map;

    std::map<TKey, TValue> overflow;

    void check_flush_overflow() {
        if (overflow.size() < overflow_size) return;

        auto block_map_iterator = file_block_map.begin();
        file_block *previous = nullptr;

        while (overflow.size() > 0) {
            file_block fb;

            if (block_map_iterator != file_block_map.end()) {
                fb = block_map_iterator->second;
                file_block_map.erase(block_map_iterator);
            }
            else {
                fb = file_block(this);
                if (previous) previous->next = &fb;
                previous = &fb;
            }

            fb.load();
            fb.merge_overflow(overflow);
            fb.store();

            auto entry = std::make_pair(key_interval(fb.get_min_key(), fb.get_max_key()), fb);
            auto insert_result = file_block_map.insert(entry);
            block_map_iterator = ++insert_result.first;
        }
    }

    TValue &get_single_key_interval(const key_interval &ki) {
        check_flush_overflow();

        // File Block lookup
        auto file_block_iterator = file_block_map.find(ki);

        // No entry => Overflow
        if (file_block_iterator == file_block_map.end()) return overflow[ki.min_key];

        // Load file block
        file_block *currently_loaded_file_block = &(file_block_iterator->second);
        currently_loaded_file_block->load();

        // Get value
        auto index = currently_loaded_file_block->find(ki.min_key);
        if (index < 0) return overflow[ki.min_key];
        else return (*currently_loaded_file_block)[index];
    }

};

template<typename TKey, typename TValue>
class isam<TKey, TValue>::iterator {
public:
    enum MODE {
        BEGIN, END
    };

    iterator() = default;

    iterator(isam<TKey, TValue>::file_block *block, MODE mode) : inner_block(block) {}

    iterator &operator++() {
        iterator it;
        return it;
    }

    const iterator operator++(int) {
        iterator it;
        return it;
    }

    isam<TKey, TValue>::key_value_pair &operator*() {}

    isam<TKey, TValue>::key_value_pair *operator->() {}

    bool operator==(const iterator &other) const {
        return (inner_block->get_block_id() == other.inner_block->get_block_id()
                && index == other.index);
    }

    bool operator!=(const iterator &other) const {
        return !operator==(other);
    }

private:
    isam<TKey, TValue>::file_block *inner_block;
    size_t index;
};

template<typename TKey, typename TValue>
typename isam<TKey, TValue>::iterator isam<TKey, TValue>::begin() {
    return isam<TKey, TValue>::iterator(nullptr, iterator::MODE::BEGIN);
};

template<typename TKey, typename TValue>
typename isam<TKey, TValue>::iterator isam<TKey, TValue>::end() {
    return isam<TKey, TValue>::iterator(nullptr, iterator::MODE::BEGIN);
};

#endif // ISAM_ISAM_HPP
