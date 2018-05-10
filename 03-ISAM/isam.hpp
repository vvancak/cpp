#ifndef ISAM_ISAM_HPP
#define ISAM_ISAM_HPP

// Vladislav Vancak

#include<map>
#include<memory>
#include"block_provider.hpp"

namespace {
    const double FILL_FACTOR_SPLIT = 0.75;
    const double FILL_FACTOR_DELIMITER = 2;

    template<typename TKey, typename TValue>
    class block {
    private:
        typedef std::pair<TKey, TValue> key_value_pair;

        size_t block_id = 0;
        size_t current_size = 0;
        size_t max_size = 0;

        void create() {
            block_id = block_provider::create_block(max_size * sizeof(key_value_pair));
        }

        void free() {
            block_provider::free_block(block_id);
        }

    public:
        class handle {
        private:
            block *block_ptr = nullptr;
            key_value_pair *values = nullptr;

            void load() {
                this->values = static_cast<key_value_pair *>(block_provider::load_block(block_ptr->block_id));
            }

            void store() {
                auto *values_void_ptr = static_cast<void *>(values);
                block_provider::store_block(block_ptr->block_id, values_void_ptr);
            }

            bool consider_upperbound(const TKey *upper_bound) {
                if (block_ptr->max_size < 2) return false;
                return (upper_bound != nullptr);
            }

        public:
            handle() = default;

            handle(handle &&other) noexcept : block_ptr(other.block_ptr),
                                              values(other.values) {
                // prevent other from saving values
                other.values = nullptr;
            }

            handle &operator=(handle &&other) {
                if (this->values && this->values != other.values) store();

                this->block_ptr = other.block_ptr;
                this->values = other.values;

                // prevent other from saving values
                other.values = nullptr;

                return *this;
            }

            explicit handle(block *block_ptr) : block_ptr(block_ptr) {
                load();
            }

            ~handle() {
                if (values) store();
            }

            const TKey &min_key() const {
                return values[0].first;
            }

            const TKey &max_key() const {
                return values[block_ptr->current_size - 1].first;
            }

            int32_t find(const TKey &key) const {
                if (max_key() < key) return -1;
                if (key < min_key()) return -1;

                int32_t upper = block_ptr->current_size - 1;
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

            key_value_pair &operator[](size_t index) const {
                return values[index];
            }

            block *get_block_ptr() const { return block_ptr; }

            handle split_block() {

                // Create new block
                auto new_block = std::make_unique<block>(block_ptr->max_size);
                auto *new_block_ptr = new_block.get();
                handle new_block_handle = new_block->load();

                // Pointers in the linked list
                new_block->next = std::move(block_ptr->next);
                block_ptr->next = std::move(new_block);

                // Move half of the values into the new block
                size_t count = block_ptr->current_size / 2;
                size_t init_index = (block_ptr->current_size) - count;
                for (size_t idx = 0; idx < count; ++idx) {
                    new_block_handle.values[idx] = values[init_index + idx];
                }

                block_ptr->current_size -= count;
                new_block_ptr->current_size = count;

                return std::move(new_block_handle);
            }

            void merge_overflow(std::map<TKey, TValue> &overflow, const TKey *upper_bound) {
                size_t index = 0;
                while (true) {
                    // block is full
                    if (index == block_ptr->max_size) break;

                        // overflow empty
                    else if (overflow.size() == 0) break;

                        // overflow and values - fill by smaller ones
                    else if (index < block_ptr->current_size) {
                        if (values[index].first < overflow.begin()->first) {
                            ++index;
                            continue;
                        }

                        overflow.emplace(values[index]);
                        auto it = overflow.begin();

                        values[index] = *it;
                        overflow.erase(it);
                    }

                        // just overflow : beyond upper bound
                    else if (consider_upperbound(upper_bound)
                             && *upper_bound < overflow.begin()->first) {
                        break;
                    }

                        // just overflow => pure insert
                    else if (index == block_ptr->current_size) {
                        block_ptr->current_size += 1;
                        auto it = overflow.begin();

                        values[index] = *it;
                        overflow.erase(it);
                    }
                    ++index;
                }
            }
        };

        std::unique_ptr<block> next = nullptr;

        explicit block(size_t max_size) : current_size(0), max_size(max_size) { create(); }

        ~block() { free(); }

        const bool is_full() const { return current_size == max_size; }

        const size_t get_size() const { return current_size; }

        bool needs_split() {
            if (max_size < FILL_FACTOR_DELIMITER) return false;
            return current_size >= (FILL_FACTOR_SPLIT * max_size);
        }

        handle load() { return handle(this); }
    };
}

template<typename TKey, typename TValue>
class isam {
private:
    typedef std::pair<TKey, TValue> key_value_pair;
    typedef block<TKey, TValue> file_block;
    typedef typename file_block::handle file_block_handle;

    const TValue default_tvalue;

    const size_t block_size;

    const size_t max_overflow_size;

    std::unique_ptr<file_block> first_file_block;

    mutable file_block *loaded_block;

    mutable file_block_handle loaded_block_handle;

    std::map<TKey, file_block *> file_block_map;

    std::map<TKey, TValue> overflow;

    void check_split_block() {
        if (!loaded_block->next) return;
        if (!loaded_block->needs_split()) return;

        auto new_block_handle = loaded_block_handle.split_block();
        auto entry = std::make_pair(new_block_handle.min_key(), new_block_handle.get_block_ptr());
        file_block_map.insert(entry);
    }

    void check_append_next() {
        if (loaded_block->next) return;
        if (!loaded_block->is_full()) return;

        auto overflow_smallest_key = overflow.begin()->first;
        if (overflow_smallest_key < loaded_block_handle.max_key()) return;

        loaded_block->next = std::make_unique<file_block>(block_size);
        loaded_block = loaded_block->next.get();
        loaded_block_handle = loaded_block->load();
    }

    void check_flush_overflow() {
        if (overflow.size() < max_overflow_size) return;

        loaded_block = nullptr;

        while (overflow.size() > 0) {

            const TKey *next_block_min_key = nullptr;
            auto overflow_smallest_key = overflow.begin()->first;
            auto block_map_iterator = file_block_map.upper_bound(overflow_smallest_key);

            // Take lower bound block out of the map
            if (block_map_iterator != file_block_map.end()) {
                if (block_map_iterator->second != first_file_block.get()) block_map_iterator--;

                loaded_block = block_map_iterator->second;

                // Next block min_key
                auto copy = block_map_iterator;
                if (++copy != file_block_map.end()) next_block_min_key = &(copy->first);

                file_block_map.erase(block_map_iterator);
            }

                // No lower bound => select last file block
            else {
                if (!first_file_block) {
                    first_file_block = std::make_unique<file_block>(block_size);
                    loaded_block = first_file_block.get();
                }
                else {
                    loaded_block = (--file_block_map.end())->second;
                }
            }

            // Load block & check adding new blocks
            loaded_block_handle = loaded_block->load();
            check_split_block();
            check_append_next();

            // Merge && Insert back into the map
            loaded_block_handle.merge_overflow(overflow, next_block_min_key);
            auto entry = std::make_pair(loaded_block_handle.min_key(), loaded_block);
            file_block_map.insert(entry);
        }
    }

    TValue &get_value(const TKey &key) {
        check_flush_overflow();

        // File Block lookup
        auto file_block_it = file_block_map.upper_bound(key);

        // No greater record found => return last block or overflow
        if (file_block_it == file_block_map.end()) {
            if (first_file_block) file_block_it = --file_block_map.end();
            else return overflow[key];
        }

            // Return the preceding file block
        else {
            if (file_block_it->second != first_file_block.get()) file_block_it--;
        }

        // Load file block
        if (loaded_block != file_block_it->second) {
            loaded_block = file_block_it->second;
            loaded_block_handle = loaded_block->load();
        }

        // Try get value from the file block (else overflow)
        auto index = loaded_block_handle.find(key);

        if (index < 0) return overflow[key];
        else return loaded_block_handle[index].second;
    }

    const TValue &get_value(const TKey &key) const {

        // File Block lookup
        auto file_block_it = file_block_map.upper_bound(key);

        // No greater record found => return last block or overflow
        if (file_block_it == file_block_map.end()) {
            if (first_file_block) file_block_it = --file_block_map.end();
            else {
                auto pos = overflow.find(key);
                if (pos != overflow.end()) return pos->second;
                else return default_tvalue;
            }
        }

            // Return the preceding file block
        else {
            if (file_block_it->second != first_file_block.get()) file_block_it--;
        }

        // Load file block
        if (loaded_block != file_block_it->second) {
            loaded_block = file_block_it->second;
            loaded_block_handle = loaded_block->load();
        }

        // Try get value from the file block (else overflow)
        auto index = loaded_block_handle.find(key);

        if (index < 0) {
            auto pos = overflow.find(key);
            if (pos != overflow.end()) return pos->second;
            else return default_tvalue;
        }
        else return loaded_block_handle[index].second;
    }

public:

    class iterator {
    private:
        typedef typename std::map<TKey, TValue>::iterator overflow_iterator;

        // overflow
        overflow_iterator overflow_it;
        overflow_iterator overflow_end;

        // file blocks
        file_block *loaded_block;
        file_block_handle loaded_block_handle;
        size_t index;


        // Determine if the next value should be taken from the overflow or from inner block
        bool take_from_overflow() {
            if (!loaded_block) return true;
            if (overflow_it == overflow_end) return false;

            // both have values => check if overflow has lower value
            return (overflow_it->first < loaded_block_handle[index].first);
        }

        void increment_logic() {
            // overflow
            if (take_from_overflow()) ++overflow_it;

                // file
            else if (++index == loaded_block->get_size()) {
                index = 0;
                loaded_block = loaded_block->next.get();

                if (loaded_block) loaded_block_handle = loaded_block->load();
                else loaded_block_handle = file_block_handle();
            }
        }

    public:
        typedef std::pair<const TKey, TValue> value_type;
        typedef value_type &reference;
        typedef value_type *pointer;
        typedef std::forward_iterator_tag iterator_category;
        typedef int32_t difference_type;

        iterator() = default;

        // Copy
        iterator(const iterator &other) : index(other.index),
                                          loaded_block(other.loaded_block),
                                          overflow_it(other.overflow_it),
                                          overflow_end(other.overflow_end) {
            if (loaded_block) loaded_block_handle = loaded_block->load();
        }

        // Copy-Assignment
        iterator &operator=(const iterator &other) {
            index = other.index;
            loaded_block = other.loaded_block;
            if (loaded_block) loaded_block_handle = loaded_block->load();

            overflow_it = other.overflow_it;
            overflow_end = other.overflow_end;

            return *this;
        }

        // Begin-Constructor
        iterator(overflow_iterator &&overflow_it, overflow_iterator &&overflow_end, file_block *loaded_block) :
                overflow_it(overflow_it),
                overflow_end(overflow_end),
                index(0),
                loaded_block(loaded_block) {
            if (loaded_block) loaded_block_handle = loaded_block->load();
        }

        // End-Constructor
        explicit iterator(overflow_iterator &&overflow_end) : overflow_it(overflow_end),
                                                              overflow_end(overflow_it),
                                                              index(0),
                                                              loaded_block(0),
                                                              loaded_block_handle() {}

        iterator &operator++() {
            increment_logic();
            return *this;
        }

        const iterator operator++(int) {
            auto copy = *this;
            increment_logic();
            return copy;
        }

        reference operator*() {
            if (take_from_overflow()) {
                return (reference) *overflow_it;
            }
            else return (reference) loaded_block_handle[index];
        }

        pointer operator->() {
            if (take_from_overflow()) {
                return &(*overflow_it);
            }
            else return (pointer) &loaded_block_handle[index];
        }

        bool operator==(const iterator &other) const {
            return (loaded_block == other.loaded_block
                    && index == other.index
                    && overflow_it == other.overflow_it
            );
        }

        bool operator!=(const iterator &other) const {
            return !(operator==(other));
        }
    };

    class const_iterator {
    private:
        typedef typename std::map<TKey, TValue>::const_iterator overflow_iterator;

        // overflow
        overflow_iterator overflow_it;
        overflow_iterator overflow_end;

        // file blocks
        file_block *loaded_block;
        file_block_handle loaded_block_handle;
        size_t index;


        // Determine if the next value should be taken from the overflow or from inner block
        bool take_from_overflow() const {
            if (!loaded_block) return true;
            if (overflow_it == overflow_end) return false;

            // both have values => check if overflow has lower value
            return (overflow_it->first < loaded_block_handle[index].first);
        }

        void increment_logic() {
            // overflow
            if (take_from_overflow()) ++overflow_it;

                // file
            else if (++index == loaded_block->get_size()) {
                index = 0;
                loaded_block = loaded_block->next.get();

                if (loaded_block) loaded_block_handle = loaded_block->load();
                else loaded_block_handle = file_block_handle();
            }
        }

    public:
        typedef std::pair<const TKey, const TValue> value_type;
        typedef value_type &reference;
        typedef value_type *pointer;
        typedef std::forward_iterator_tag iterator_category;
        typedef int32_t difference_type;

        const_iterator() = default;

        // Copy
        const_iterator(const const_iterator &other) : index(other.index),
                                                      loaded_block(other.loaded_block),
                                                      overflow_it(other.overflow_it),
                                                      overflow_end(other.overflow_end) {
            if (loaded_block) loaded_block_handle = loaded_block->load();
        }

        // Copy-Assign
        const_iterator &operator=(const const_iterator &other) {
            index = other.index;
            loaded_block = other.loaded_block;
            if (loaded_block) loaded_block_handle = loaded_block->load();

            overflow_it = other.overflow_it;
            overflow_end = other.overflow_end;

            return *this;
        }

        // Begin-Constructor
        const_iterator(overflow_iterator &&overflow_it, overflow_iterator &&overflow_end, file_block *loaded_block) :
                overflow_it(overflow_it),
                overflow_end(overflow_end),
                index(0),
                loaded_block(loaded_block) {
            if (loaded_block) loaded_block_handle = loaded_block->load();
        }

        // End-Constructor
        explicit const_iterator(overflow_iterator &&overflow_end) : overflow_it(overflow_end),
                                                                    overflow_end(overflow_it),
                                                                    index(0),
                                                                    loaded_block(0),
                                                                    loaded_block_handle() {}

        const_iterator &operator++() {
            increment_logic();
            return *this;
        }

        const const_iterator operator++(int) {
            auto copy = *this;
            increment_logic();
            return copy;
        }

        reference operator*() const {
            if (take_from_overflow()) {
                return (reference) *overflow_it;
            }
            else return (reference) loaded_block_handle[index];
        }

        pointer operator->() const {
            if (take_from_overflow()) {
                return &(*overflow_it);
            }
            else return (pointer) &loaded_block_handle[index];
        }

        bool operator==(const const_iterator &other) const {
            return (loaded_block == other.loaded_block
                    && index == other.index
                    && overflow_it == other.overflow_it
            );
        }

        bool operator!=(const const_iterator &other) const {
            return !(operator==(other));
        }
    };

    isam(size_t
         block_size,
         size_t overflow_size
    ) :

            block_size(block_size),
            max_overflow_size(overflow_size),

            default_tvalue(),

            overflow{},
            file_block_map{},

            loaded_block(),
            loaded_block_handle() {}

    // ~isam() { first_file_block.release(); }

    TValue &operator[](const TKey &key) {
        return get_value(key);
    }

    TValue &operator[](TKey &&key) {
        return get_value(key);
    }

    const TValue &operator[](const TKey &key) const {
        return get_value(key);
    }

    const TValue &operator[](TKey &&key) const {
        return get_value(key);
    }

    iterator begin() {
        file_block *fb_ptr = first_file_block ? first_file_block.get() : nullptr;
        return iterator(overflow.begin(), overflow.end(), fb_ptr);
    }

    const_iterator begin() const {
        file_block *fb_ptr = first_file_block ? first_file_block.get() : nullptr;
        return const_iterator(overflow.begin(), overflow.end(), fb_ptr);
    }

    iterator end() {
        return iterator(overflow.end());
    }

    const_iterator end() const {
        return const_iterator(overflow.end());
    }

};

#endif // ISAM_ISAM_HPP