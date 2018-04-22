
#ifndef _ii_hpp
#define _ii_hpp

#include <iostream>
#include <cstdint>
#include <queue>
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>

namespace ii {
    namespace {
        typedef uint64_t DocumentId;
        typedef uint64_t FeatureId;

        namespace CompressionHelpers {
            // Data Compression - each byte consists of :
            // - 1 bit marks, if the value should be appended to the previous byte
            // - 7 bits of value

            const uint8_t BITMASK_HAS_NEXT = 0x80;
            const uint8_t BITMASK_LAST_7_BITS = 0x7F;
            const uint8_t BITMASK_NOT_NEXT = 0x00;

            uint64_t store_next(const DocumentId &last, DocumentId next, uint8_t *data_ptr) {
                uint64_t bit_count = 0;
                uint8_t tag_continue = BITMASK_NOT_NEXT;

                uint64_t value = next - last;

                while (value > 0) {
                    auto last_7_bits = uint8_t(value & BITMASK_LAST_7_BITS);

                    *data_ptr = tag_continue | last_7_bits;
                    value = value >> 7;

                    ++data_ptr;
                    ++bit_count;
                    tag_continue = BITMASK_HAS_NEXT;
                }

                return bit_count;
            }

            uint64_t get_byte_count(const uint8_t *data_ptr) {
                uint64_t bc = 1;
                for (auto i = 1; *(data_ptr + i) > BITMASK_HAS_NEXT; ++i) ++bc;
                return bc;
            }

            uint64_t get_next(const uint64_t &last_document_id, uint8_t *data_ptr) {
                uint64_t value = 0;

                for (auto i = 0; i < get_byte_count(data_ptr); ++i) {
                    uint64_t next_byte = *(data_ptr + i) & BITMASK_LAST_7_BITS;
                    value = value | next_byte << (i * 7);
                }
                return last_document_id + value;
            }
        }

        class Storage {
        public:
            class FeatureDocuments {
            public:
                FeatureDocuments() {}

                FeatureDocuments(Storage *storage, FeatureId feature_id) : storage(storage),
                                                                           feature_id(feature_id) {}

                FeatureDocuments(std::vector<DocumentId> &&vct) : storage(),
                                                                  feature_id(),
                                                                  documents_vector(vct) {}

                struct Iterator {
                    Iterator(uint8_t *data_start) : data_ptr(data_start),
                                                    last_document(0),
                                                    buffer_iterator() {}

                    Iterator(std::vector<DocumentId>::iterator &&buffer_iterator) :
                            data_ptr(),
                            last_document(0),
                            buffer_iterator(buffer_iterator) {}

                    Iterator &operator++() {
                        if (data_ptr) {
                            last_document = CompressionHelpers::get_next(last_document, data_ptr);
                            data_ptr += CompressionHelpers::get_byte_count(data_ptr);
                        } else {
                            buffer_iterator++;
                        }
                        return *this;
                    }

                    Iterator operator++(int) {
                        Iterator i = *this;
                        if (data_ptr) {
                            last_document = CompressionHelpers::get_next(last_document, data_ptr);
                            data_ptr += CompressionHelpers::get_byte_count(data_ptr);
                        } else {
                            buffer_iterator++;
                        }
                        return i;
                    }

                    DocumentId operator*() {
                        if (data_ptr) return CompressionHelpers::get_next(last_document, data_ptr);
                        else return *buffer_iterator;

                    }

                    bool operator==(const Iterator &rhs) {
                        return data_ptr == rhs.data_ptr && buffer_iterator == rhs.buffer_iterator;
                    }

                    bool operator!=(const Iterator &rhs) {
                        return data_ptr != rhs.data_ptr || buffer_iterator != rhs.buffer_iterator;
                    }

                private:
                    std::uint8_t *data_ptr;
                    DocumentId last_document;
                    std::vector<DocumentId>::iterator buffer_iterator;
                };

                Iterator begin() {
                    if (storage) {
                        auto *entry = storage->get<Entry>(feature_id);
                        auto *first_document = storage->get<uint8_t>(entry->document_offset);
                        return Iterator(first_document);
                    } else {
                        return Iterator(documents_vector.begin());
                    }
                }

                Iterator end() {
                    if (storage) {
                        auto *entry = storage->get<Entry>(feature_id);
                        auto *behind_last = storage->get<uint8_t>(entry->document_offset + entry->count);
                        return Iterator(behind_last);
                    } else {
                        return Iterator(documents_vector.end());
                    }
                }

            private:
                FeatureId feature_id;
                Storage *storage;
                std::vector<DocumentId> documents_vector;
            };

            Storage(const uint64_t *data_file_ptr) : data_ptr(data_file_ptr) {}

            FeatureDocuments operator[](FeatureId id) {
                return FeatureDocuments(this, id);
            }

        protected:
            struct Entry {
                FeatureId id;
                uint64_t count;
                uint64_t document_offset;
            };

            template<typename T>
            T *get(uint64_t offset) {
                return static_cast<T *>(const_cast<void *>(data_ptr)) + offset;
            }

            const void *data_ptr;
        };

        class Writer : Storage {
        public:
            static uint64_t get_maximum_datafile_size(uint64_t feature_count, uint64_t total_data) {
                return (sizeof(Entry) * feature_count) + (sizeof(DocumentId) * total_data);
            }

            Writer(const uint64_t *data_file_ptr, uint64_t feature_count) :
                    Storage(data_file_ptr),
                    feature_count(feature_count) {
                next_document_offset = (feature_count * sizeof(Entry));
            }

            template<typename IT>
            void persist(FeatureId id, IT &&documents) {
                auto *next_document = get<uint8_t>(next_document_offset);

                uint64_t total_bits = 0;
                uint64_t last_document_id = 0;
                for (DocumentId &&document_id : documents) {
                    uint64_t bit_count = CompressionHelpers::store_next(last_document_id, document_id, next_document);

                    last_document_id = document_id;
                    next_document += bit_count;
                    total_bits += bit_count;
                }

                unflushed_entries.push_back(Entry{id, total_bits, next_document_offset});
                if (unflushed_entries.size() > BUFFER_SIZE) flush();

                next_document_offset += total_bits;
            }

            uint64_t get_current_document_size() {
                return next_document_offset;
            }

            void flush() {
                auto *next_entry = get<Entry>(next_entry_offset);
                for (Entry &fe : unflushed_entries) {
                    ++next_entry_offset;
                    *(next_entry++) = fe;
                }
                unflushed_entries.clear();
            }

        private:
            const size_t BUFFER_SIZE = 20;

            uint64_t feature_count = 0;
            uint64_t next_entry_offset = 0;
            uint64_t next_document_offset = 0;
            std::vector<Entry> unflushed_entries{};
        };

        class Processor {
        public:
            Processor(Storage &storage) : storage(storage) {}

            template<typename FS>
            Storage::FeatureDocuments search(FS &&query_features) {
                for (auto &&feature_id : query_features) processing_queue.push(storage[feature_id]);

                const uint32_t hw_thread_count = std::thread::hardware_concurrency();
                const uint32_t process_threads = hw_thread_count < 8 ? hw_thread_count : 8;

                std::vector<std::thread> workers;
                for (int i = 0; i < process_threads; ++i)
                    workers.push_back(std::thread([this]() { main_thread_worker_job(); }));

                for (auto &t : workers)
                    t.join();

                return processing_queue.front();
            }

        private:
            Storage &storage;
            uint64_t processing = 0;
            std::mutex queue_mutex;
            std::condition_variable condition_variable;
            std::queue<Storage::FeatureDocuments> processing_queue;

            Storage::FeatureDocuments merge(Storage::FeatureDocuments &first, Storage::FeatureDocuments &second) {
                std::vector<DocumentId> result_vector;

                auto it_first = first.begin();
                auto it_second = second.begin();

                while (it_first != first.end() && it_second != second.end()) {

                    // Equal => result
                    if (*it_first == *it_second) {
                        result_vector.push_back(*it_first);
                        ++it_first;
                        ++it_second;
                    }

                        // First shift
                    else if (*it_first < *it_second) ++it_first;

                        // Second Shift
                    else if (*it_first > *it_second) ++it_second;
                }

                return Storage::FeatureDocuments(std::move(result_vector));
            }

            void main_thread_worker_job() {
                while (true) {
                    Storage::FeatureDocuments first_buffer;
                    Storage::FeatureDocuments second_buffer;

                    std::unique_lock lock(queue_mutex);
                    while (processing_queue.size() < 2) {

                        // END
                        if (processing == 0) goto outer_while_end;

                        // Wait for more elments
                        condition_variable.wait(lock);
                    }

                    // Get to-merge lists
                    first_buffer = processing_queue.front();
                    processing_queue.pop();

                    second_buffer = processing_queue.front();
                    processing_queue.pop();

                    // Synchronization - processing start
                    ++processing;
                    lock.unlock();

                    // Process
                    Storage::FeatureDocuments result = merge(first_buffer, second_buffer);

                    // Store
                    {
                        std::lock_guard guard(queue_mutex);
                        processing_queue.push(std::move(result));
                        --processing;
                    }
                    condition_variable.notify_one();
                }
                outer_while_end:

                // Processing end (for at least one thread) => Notify all other waiting threads to terminate themselves
                condition_variable.notify_all();
            }
        };
    }; // namespace

    template<typename Truncate, typename FeatureObjectLists>
    void create(Truncate &&truncate, FeatureObjectLists &&features) {
        // Get data file size
        uint64_t total_data = 0;
        for (uint64_t i = 0; i < features.size(); ++i) {
            for (auto &&_ : features[i]) ++total_data;
        }

        // Create the data file
        uint64_t data_file_size = Writer::get_maximum_datafile_size(features.size(), total_data);
        uint64_t *data_file = truncate(data_file_size);

        // Persist the feature data
        Writer writer(data_file, features.size());

        for (FeatureId id = 0; id < features.size(); ++id) {
            writer.persist(id, features[id]);
        }
        writer.flush();

        // Truncate data file to the actual size (drop pre-allocated space)
        truncate(writer.get_current_document_size());
    }

    template<class Fs, class OutFn>
    void search(const uint64_t *segment, size_t size, Fs &&fs, OutFn &&callback) {
        Storage features(segment);
        Processor processor(features);

        auto result = processor.search(fs);
        for (auto &&document_id: result) callback(document_id);
    }

}; //namespace ii

#endif
