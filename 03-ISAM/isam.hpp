#ifndef ISAM_ISAM_HPP
#define ISAM_ISAM_HPP

// Vladislav Vancak

#include<map>
#include<list>
#include<memory>
#include"block_provider.hpp"

template<typename TKey, typename TValue>
class isam {
private:
    class file_block {
    private:
        typedef std::pair<TKey, TValue> kvp_inner;
        typedef std::pair<const TKey &, TValue &> kvp_out;
        typedef std::pair<const TKey &, const TValue &> kvp_out_const;

        size_t block_id = 0;
        size_t current_size = 0;

        isam *isam_members = 0;
        kvp_inner *values{};

        std::unique_ptr<kvp_out> last_out;
        mutable std::unique_ptr<kvp_out_const> last_out_const;
    public:
        std::unique_ptr<file_block> next = 0;

        file_block() = default;

        explicit file_block(isam *const parent) :
                last_out(nullptr),
                last_out_const(nullptr),
                current_size(0),
                isam_members(parent) {
            block_id = block_provider::create_block(isam_members->block_size * sizeof(kvp_inner));
        }

        void load() {
            values = static_cast<kvp_inner *>(block_provider::load_block(block_id));
        }

        void store() {
            auto *block_ptr = static_cast<void *>(values);
            block_provider::store_block(block_id, block_ptr);
        }

        void free() {
            block_provider::free_block(block_id);
        }

        const TKey &get_min_key() const {
            return values[0].first;
        }

        const TKey &get_max_key() const {
            return values[current_size - 1].first;
        }

        const size_t get_size() const {
            return current_size;
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

        std::pair<const TKey &, TValue &> &get_noconst(size_t index) {
            if (last_out) last_out.release();
            last_out = std::make_unique<kvp_out>(values[index].first, values[index].second);
            return *last_out;
        }

        std::pair<const TKey &, const TValue &> get_const(size_t index) const {
            if (last_out_const) last_out_const.release();
            last_out_const = std::make_unique<kvp_out_const>(values[index].first, values[index].second);
            return *last_out_const;
        };

        void merge_overflow(std::map<TKey, TValue> &overflow) {
            const size_t max_size = isam_members->block_size;

            // Create a file_block_map with current values
            std::map<TKey, TValue> block_values;
            for (size_t i = 0; i < current_size; ++i) {
                block_values.emplace(values[i]);
            }

            // Merge
            while (true) {
                // Nothing to merge
                if (overflow.size() == 0) break;

                // Full block & correct order
                if (current_size == max_size && (--block_values.end())->first < overflow.begin()->first) break;

                // can push more values => emplace values from overflow
                if (current_size <= max_size && overflow.size() > 0) {
                    block_values.emplace(*overflow.begin());
                    overflow.erase(overflow.begin());

                    ++current_size;
                    continue;
                }

                // too much values => move max to overflow
                if (current_size > max_size) {
                    overflow.emplace(*(--block_values.end()));
                    block_values.erase(--block_values.end());

                    --current_size;
                    continue;
                }
            }

            // Store values
            size_t idx = 0;
            for (auto &&kvp : block_values) {
                values[idx++] = kvp;
            }
        }
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

    const size_t max_overflow_size;

    std::unique_ptr<file_block> first_file_block;

    file_block *currently_loaded_file_block;

    std::map<key_interval, file_block *, key_interval_comparator> file_block_map;

    std::map<TKey, TValue> overflow;

    void check_flush_overflow() {
        if (overflow.size() < max_overflow_size) return;

        // Clear currently loaded block
        if (currently_loaded_file_block) currently_loaded_file_block->store();
        currently_loaded_file_block = nullptr;

        // First file block
        auto block_map_iterator = file_block_map.begin();
        file_block *current_file_block = first_file_block.get();

        // Main merge
        while (overflow.size() > 0) {

            // Take the next file block out of the map
            if (block_map_iterator != file_block_map.end()) {
                current_file_block = block_map_iterator->second;
                file_block_map.erase(block_map_iterator);
            }

                // Create the next file block -- not the first file block
            else if (current_file_block) {
                current_file_block->next = std::make_unique<file_block>(this);
                current_file_block = current_file_block->next.get();
            }

                // Create the next file block -- first file block
            else {
                first_file_block = std::make_unique<file_block>(this);
                current_file_block = first_file_block.get();
            }

            // Load & merge values into the block
            current_file_block->load();
            current_file_block->merge_overflow(overflow);
            current_file_block->store();

            auto entry = std::make_pair(
                    key_interval(current_file_block->get_min_key(), current_file_block->get_max_key()),
                    current_file_block
            );

            auto insert_result = file_block_map.insert(entry);
            block_map_iterator = ++insert_result.first;
        }
    }

    TValue &get_interval_value(const key_interval &ki) {
        check_flush_overflow();

        // File Block lookup
        auto file_block_iterator = file_block_map.find(ki);

        // No entry => Overflow
        if (file_block_iterator == file_block_map.end()) return overflow[ki.min_key];

        // Load file block
        if (currently_loaded_file_block) currently_loaded_file_block->store();
        currently_loaded_file_block = file_block_iterator->second;
        currently_loaded_file_block->load();

        // Try get value from the file block
        auto index = currently_loaded_file_block->find(ki.min_key);
        if (index < 0) return overflow[ki.min_key];
        else return currently_loaded_file_block->get_noconst(index).second;
    }

public:
/*
    class iterator {
    private:
        isam::file_block *inner_block;
        std::map<TKey, TValue> *overflow;
        typename std::map<TKey, TValue>::iterator overflow_it;
        isam<TKey, TValue>::kvp_inner overflow_return_value;
        size_t index;

        // Determine if the next value should be taken from the overflow or from inner block
        bool take_from_overflow() {
            if (!inner_block) return true;
            if (overflow_it == overflow->end()) return false;

            if (!inner_block->next && index == inner_block->get_size()) return true;
            return (overflow_it->first < (*inner_block)[index].first);
        }

    public:
        iterator() = default;

        iterator(isam::file_block *block,
                 std::map<TKey, TValue> *overflow,
                 bool is_end = false) : inner_block(block),
                                        index(is_end ? block->get_size() : 0),
                                        overflow(overflow),
                                        overflow_it(is_end ? overflow->end() : overflow->begin()) {
            if (block) inner_block->load();
            if (overflow_it != overflow->end()) overflow_return_value = *overflow_it;
        }

        ~iterator() {
            if (inner_block) inner_block->store();
            if (overflow_it != overflow->end()) overflow_it->second = overflow_return_value.second;

        }

        iterator &operator++() {
            increment_logic();
            return *this;
        }

        const iterator operator++(int) {
            auto copy = *this;
            increment_logic();
            return copy;
        }

        void increment_logic() {
            // index out of range => only overflow left
            if (take_from_overflow()) {
                overflow_it->second = overflow_return_value.second;
                ++overflow_it;
            }

                // increase index of the inner file - load & store next might be necessary
            else if (++index == inner_block->get_size() && inner_block->next) {
                inner_block->store();
                inner_block = inner_block->next;
                inner_block->load();
                index = 0;
            }
        }

        isam::kvp_inner &operator*() {
            if (take_from_overflow()) {
                overflow_return_value = *overflow_it;
                return overflow_return_value;
            }
            else return (*inner_block)[index];
        }

        isam::kvp_inner *operator->() {
            if (take_from_overflow()) {
                overflow_return_value = *overflow_it;
                return &overflow_return_value;
            }
            else return &(*inner_block)[index];
        }

        bool operator==(const iterator &other) const {
            return (inner_block == other.inner_block
                    && index == other.index
                    && overflow_it == other.overflow_it
            );
        }

        bool operator!=(const iterator &other) const {
            return !operator==(other);
        }
    };
*/
    isam(size_t
         block_size,
         size_t overflow_size
    ) :
            currently_loaded_file_block(nullptr),
            block_size(block_size),
            max_overflow_size(overflow_size),
            file_block_map{},
            overflow{} {}

    TValue &operator[](const TKey &key) {
        key_interval ki(key);
        return get_interval_value(ki);
    }

    TValue &operator[](TKey &&key) {
        key_interval ki(key);
        return get_interval_value(ki);
    }
/*
    iterator begin() {
        // valid file blocks available
        if (file_block_map.size() > 0) {
            file_block *fb = file_block_map.begin()->second;
            return iterator(fb, &overflow, false);
        }

            // just overflow
        else return iterator(nullptr, &overflow, false);
    }

    iterator end() {
        // valid file blocks available
        if (file_block_map.size() > 0) {
            auto before_end = --(file_block_map.end());
            file_block *fb = before_end->second;
            return iterator(fb, &overflow, true);
        }
            // Just overflow
        else return iterator(nullptr, &overflow, true);
    }
*/
};

#endif // ISAM_ISAM_HPP
