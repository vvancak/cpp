
#ifndef _ii_hpp
#define _ii_hpp

#include <iostream>
#include <cstdint>
#include <stdint-gcc.h>
#include <functional>
#include <queue>

namespace ii {

    typedef uint64_t DocumentId;
    typedef uint64_t FeatureId;

    const size_t BUFFER_SIZE = 20;

    namespace {
        class Storage {
        public:
            struct DocumentIterator {
                DocumentIterator(DocumentId *_data_start) : data_ptr(_data_start) {}

                DocumentIterator &operator++() {
                    data_ptr++;
                    return *this;
                }

                DocumentIterator operator++(int junk) {
                    DocumentIterator i = *this;
                    data_ptr++;
                    return i;
                }

                DocumentId &operator*() { return *data_ptr; }

                bool operator==(const DocumentIterator &rhs) { return data_ptr == rhs.data_ptr; }

                bool operator!=(const DocumentIterator &rhs) { return data_ptr != rhs.data_ptr; }

            private:
                DocumentId *data_ptr;
            };

            struct FeatureDocuments {
            public:
                FeatureDocuments(Storage *storage, FeatureId id) : _id(id), _storage(storage) {}

                DocumentIterator begin() {
                    auto *entry = _storage->get<FeatureEntry>(_id);
                    auto *first_document = _storage->get<DocumentId>(entry->document_offset);
                    return DocumentIterator(first_document);
                }

                DocumentIterator end() {
                    auto *entry = _storage->get<FeatureEntry>(_id);
                    auto *last_document = _storage->get<DocumentId>(entry->document_offset + entry->count);
                    return DocumentIterator(last_document);
                }

            private:
                Storage *_storage;
                FeatureId _id;
            };

            static uint64_t get_datafile_size(uint64_t feature_count, uint64_t total_data) {
                return (sizeof(FeatureEntry) * feature_count) + total_data;
            }

            Storage(const uint64_t *data_file_ptr) : _data_ptr(data_file_ptr) {}

            FeatureDocuments operator[](FeatureId id) {
                return FeatureDocuments(this, id);
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

        template<typename IT1, typename IT2>
        std::vector<DocumentId> merge(IT1 iterable_first, IT2 iterable_second) {
            std::vector<DocumentId> result_vector;

            auto it_first = iterable_first.begin();
            auto it_first_end = iterable_first.end();

            auto it_second = iterable_second.begin();
            auto it_second_end = iterable_second.end();

            while (it_first != it_first_end && it_second != it_second_end) {

                if (*it_first == *it_second) {
                    result_vector.push_back(*it_first);
                    ++it_first;
                    ++it_second;
                    continue;
                }

                if (*it_first > *it_second) {
                    ++it_second;
                    continue;
                }

                if (*it_first < *it_second) {
                    ++it_first;
                    continue;
                }
            }

            return result_vector;
        }
    };

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
        for (int i = 0; i < 10; ++i) {
            for (auto &&id : features[i]) std::cout << id << " ";
            std::cout << std::endl;
        }
    }

}; //namespace ii

#endif
