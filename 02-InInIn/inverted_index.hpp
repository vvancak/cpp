
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
    const size_t BUFFER_SIZE = 20;

    namespace {
        typedef uint64_t DocumentId;
        typedef uint64_t FeatureId;

        class Storage {
        public:
            class DocumentBuffer {
            public:
                DocumentBuffer() {}

                DocumentBuffer(Storage *storage, FeatureId feature_id) : storage(storage),
                                                                         feature_id(feature_id) {}

                DocumentBuffer(std::vector<DocumentId> &&vct) : storage(),
                                                                feature_id(),
                                                                documents_vector(vct) {}

                struct DocumentIterator {
                    DocumentIterator(DocumentId *data_start) : data_ptr(data_start),
                                                               buffer_iterator() {}

                    DocumentIterator(std::vector<DocumentId>::iterator &&buffer_iterator) :
                            data_ptr(),
                            buffer_iterator(buffer_iterator) {}

                    DocumentIterator &operator++() {
                        if (data_ptr) data_ptr++;
                        else buffer_iterator++;
                        return *this;
                    }

                    DocumentIterator operator++(int junk) {
                        DocumentIterator i = *this;
                        if (data_ptr) data_ptr++;
                        else buffer_iterator++;
                        return i;
                    }

                    DocumentId &operator*() {
                        if (data_ptr) return *data_ptr;
                        else return *buffer_iterator;
                    }

                    bool operator==(const DocumentIterator &rhs) {
                        return data_ptr == rhs.data_ptr && buffer_iterator == rhs.buffer_iterator;
                    }

                    bool operator!=(const DocumentIterator &rhs) {
                        return data_ptr != rhs.data_ptr || buffer_iterator != rhs.buffer_iterator;
                    }

                private:
                    DocumentId *data_ptr;
                    std::vector<DocumentId>::iterator buffer_iterator;
                };

                DocumentIterator begin() {
                    if (storage) {
                        auto *entry = storage->get<FeatureEntry>(feature_id);
                        auto *first_document = storage->get<DocumentId>(entry->document_offset);
                        return DocumentIterator(first_document);
                    } else {
                        return DocumentIterator(documents_vector.begin());
                    }
                }

                DocumentIterator end() {
                    if (storage) {
                        auto *entry = storage->get<FeatureEntry>(feature_id);
                        auto *behind_last = storage->get<DocumentId>(entry->document_offset + entry->count);
                        return DocumentIterator(behind_last);
                    } else {
                        return DocumentIterator(documents_vector.end());
                    }
                }

            private:
                FeatureId feature_id;
                Storage *storage;
                std::vector<DocumentId> documents_vector;
            };

            static uint64_t get_datafile_size(uint64_t feature_count, uint64_t total_data) {
                return (sizeof(FeatureEntry) * feature_count) + total_data;
            }

            Storage(const uint64_t *data_file_ptr) : _data_ptr(data_file_ptr) {}

            DocumentBuffer operator[](FeatureId id) {
                return DocumentBuffer(this, id);
            }

        protected:
            struct FeatureEntry {
                FeatureId id;
                uint64_t count;
                uint64_t document_offset;
            };

            template<typename T>
            T *get(uint64_t offset) {
                return static_cast<T *>(const_cast<void *>(_data_ptr)) + offset;
            }

            const void *_data_ptr;
        };

        class Writer : Storage {
        public:
            Writer(const uint64_t *data_file_ptr, uint64_t feature_count) :
                    Storage(data_file_ptr),
                    feature_count(feature_count) {
                _next_document_offset = (feature_count * sizeof(FeatureEntry)) / sizeof(DocumentId);
            }

            template<typename IT>
            void persist(FeatureId id, IT &&documents) {
                auto *next_document = get<DocumentId>(_next_document_offset);

                uint64_t total_documents = 0;
                for (DocumentId &&fd : documents) {
                    ++total_documents;
                    *(next_document++) = fd;
                }

                _unflushed_entries.push_back(FeatureEntry{id, total_documents, _next_document_offset});
                if (_unflushed_entries.size() > BUFFER_SIZE) flush();

                _next_document_offset += total_documents;
            }

            void flush() {
                auto *next_entry = get<FeatureEntry>(_next_entry_offset);
                for (FeatureEntry &fe : _unflushed_entries) {
                    ++_next_entry_offset;
                    *(next_entry++) = fe;
                }
                _unflushed_entries.clear();
            }

        private:
            uint64_t feature_count = 0;
            uint64_t _next_entry_offset = 0;
            uint64_t _next_document_offset = 0;
            std::vector<FeatureEntry> _unflushed_entries{};
        };

        class Processor {
        public:
            Processor(Storage &storage) : storage(storage) {}

            template<typename FS>
            Storage::DocumentBuffer search(FS &&query_features) {
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
            std::atomic<uint64_t> processing = 0;
            std::mutex queue_mutex;
            std::condition_variable condition_variable;
            std::queue<Storage::DocumentBuffer> processing_queue;

            Storage::DocumentBuffer merge(Storage::DocumentBuffer &first, Storage::DocumentBuffer &second) {
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

                return Storage::DocumentBuffer(std::move(result_vector));
            }

            void main_thread_worker_job() {
                while (true) {
                    Storage::DocumentBuffer first_buffer;
                    Storage::DocumentBuffer second_buffer;

                    std::unique_lock lock(queue_mutex);
                    while (processing_queue.size() < 2) {

                        // END
                        if (processing == 0) goto function_end;

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

                    // Process & Store
                    Storage::DocumentBuffer result = merge(first_buffer, second_buffer);
                    {
                        std::lock_guard guard(queue_mutex);
                        processing_queue.push(std::move(result));
                        --processing;
                        condition_variable.notify_one();
                    }
                }
                function_end:

                // One has ended => Notify all waiting threads to terminate themselves
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
        uint64_t data_file_size = Storage::get_datafile_size(features.size(), total_data);
        uint64_t *data_file = truncate(data_file_size);

        // Persist the feature data
        Writer writer(data_file, features.size());

        for (FeatureId id = 0; id < features.size(); ++id) {
            writer.persist(id, features[id]);
        }
        writer.flush();
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
